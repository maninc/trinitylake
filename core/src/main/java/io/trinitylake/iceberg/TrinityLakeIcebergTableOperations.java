/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trinitylake.iceberg;

import io.trinitylake.ObjectDefinitions;
import io.trinitylake.RunningTransaction;
import io.trinitylake.TrinityLake;
import io.trinitylake.models.TableDef;
import io.trinitylake.relocated.com.google.common.base.Objects;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.util.ValidationUtil;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.SnapshotIdGeneratorUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinityLakeIcebergTableOperations implements TableOperations, Serializable {

  private static final Logger LOG =
      LoggerFactory.getLogger(TrinityLakeIcebergTableOperations.class);

  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";

  private LakehouseStorage storage;
  private RunningTransaction transaction;
  private String tableName;
  private String namespaceName;
  private String fullTableName;
  // nullable
  private String distTransactionId;

  private FileIO fileIO;

  private TableMetadata currentMetadata = null;
  private TableDef currentTableDef = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  public TrinityLakeIcebergTableOperations(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName,
      Optional<String> distTransactionId,
      Map<String, String> allProperties) {
    this.storage = storage;
    this.transaction = transaction;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.fullTableName = String.format("%s.%s", namespaceName, tableName);
    this.distTransactionId = distTransactionId.orElse(null);
    this.fileIO = initializeFileIO(allProperties);
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String ioImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL);
    return CatalogUtil.loadFileIO(ioImpl, properties, null);
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  public TableMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      this.currentTableDef =
          TrinityLake.describeTable(storage, transaction, namespaceName, tableName);
      this.currentMetadataLocation = TrinityLakeToIceberg.tableMetadataLocation(currentTableDef);
      loadMetadata(currentMetadataLocation);
    } catch (NoSuchTableException e) {
      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
        shouldRefresh = true;
      }

      currentMetadata = null;
      currentTableDef = null;
      version = -1;
      throw e;
    }
    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      if (base != null) {
        throw new CommitFailedException("Cannot commit: stale table metadata");
      } else {
        // when current is non-null, the table exists. but when base is null, the commit is trying
        // to create the table
        throw new AlreadyExistsException("Table already exists: %s", fullTableName);
      }
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();

    String newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);
    TableDef.Builder newTableDef =
        ObjectDefinitions.newTableDefBuilder()
            .mergeFrom(currentTableDef)
            .putFormatProperties(
                TrinityLakeToIceberg.METADATA_LOCATION_FORMAT_PROPERTY, newMetadataLocation);

    if (currentMetadataLocation != null) {
      newTableDef.putFormatProperties(
          TrinityLakeToIceberg.PREVIOUS_METADATA_LOCATION_FORMAT_PROPERTY, currentMetadataLocation);
    }

    transaction =
        TrinityLake.alterTable(storage, transaction, namespaceName, tableName, newTableDef.build());

    if (distTransactionId != null) {
      TrinityLake.saveDistTransaction(storage, transaction);
    } else {
      TrinityLake.commitTransaction(storage, transaction);
    }

    shouldRefresh = true;
    LOG.info(
        "Successfully committed to table {} in {} ms",
        fullTableName,
        System.currentTimeMillis() - start);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public EncryptionManager encryption() {
    return PlaintextEncryptionManager.instance();
  }

  protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
    return newTable && metadata.metadataFileLocation() != null
        ? metadata.metadataFileLocation()
        : writeNewMetadata(metadata, version + 1);
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    TableMetadataParser.overwrite(metadata, newMetadataLocation);

    return newMetadataLocation.location();
  }

  protected void loadMetadata(String newLocation) {
    loadMetadata(newLocation, null, 20);
  }

  protected void loadMetadata(
      String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    loadMetadata(
        newLocation,
        shouldRetry,
        numRetries,
        metadataLocation -> TableMetadataParser.read(io(), metadataLocation));
  }

  protected void loadMetadata(
      String newLocation,
      Predicate<Exception> shouldRetry,
      int numRetries,
      Function<String, TableMetadata> metadataLoader) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Loading table metadata from location: {}", newLocation);

      AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newLocation)
          .retry(numRetries)
          .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .throwFailureWhenFinished()
          .stopRetryOn(NotFoundException.class) // overridden if shouldRetry is non-null
          .shouldRetryTest(shouldRetry)
          .run(metadataLocation -> newMetadata.set(metadataLoader.apply(metadataLocation)));

      String newUUID = newMetadata.get().uuid();
      if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
        ValidationUtil.checkState(
            newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s",
            currentMetadata.uuid(),
            newUUID);
      }

      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newLocation;
      this.version = parseVersion(newLocation);
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s", metadata.location(), filename);
    }
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return TrinityLakeIcebergTableOperations.this.metadataFileLocation(
            uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return TrinityLakeIcebergTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return TrinityLakeIcebergTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return TrinityLakeIcebergTableOperations.this.newSnapshotId();
      }
    };
  }

  @Override
  public long newSnapshotId() {
    return SnapshotIdGeneratorUtil.generateSnapshotID();
  }

  @Override
  public boolean requireStrictCleanup() {
    return true;
  }

  /**
   * Attempt to load the table and see if any current or past metadata location matches the one we
   * were attempting to set. This is used as a last resort when we are dealing with exceptions that
   * may indicate the commit has failed but are not proof that this is the case. Past locations must
   * also be searched on the chance that a second committer was able to successfully commit on top
   * of our commit.
   *
   * @param newMetadataLocation the path of the new commit file
   * @param config metadata to use for configuration
   * @return Commit Status of Success, Failure or Unknown
   */
  protected CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata config) {
    return CommitStatus.valueOf(
        checkCommitStatus(
                fullTableName,
                newMetadataLocation,
                config.properties(),
                () -> checkCurrentMetadataLocation(newMetadataLocation))
            .name());
  }

  /**
   * Validate if the new metadata location is the current metadata location or present within
   * previous metadata files.
   *
   * @param newMetadataLocation newly written metadata location
   * @return true if the new metadata location is the current metadata location or present within
   *     previous metadata files.
   */
  private boolean checkCurrentMetadataLocation(String newMetadataLocation) {
    TableMetadata metadata = refresh();
    String currentMetadataFileLocation = metadata.metadataFileLocation();
    return currentMetadataFileLocation.equals(newMetadataLocation)
        || metadata.previousFiles().stream()
            .anyMatch(log -> log.file().equals(newMetadataLocation));
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName =
        meta.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(
        meta,
        String.format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  /**
   * Parse the version from table metadata file name.
   *
   * @param metadataLocation table metadata file location
   * @return version of the table metadata file in success case and -1 if the version is not
   *     parsable (as a sign that the metadata is not part of this catalog)
   */
  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem table's metadata
      return -1;
    }

    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  public enum CommitStatus {
    FAILURE,
    SUCCESS,
    UNKNOWN
  }

  /**
   * Attempt to load the content and see if any current or past metadata location matches the one we
   * were attempting to set. This is used as a last resort when we are dealing with exceptions that
   * may indicate the commit has failed but don't have proof that this is the case. Note that all
   * the previous locations must also be searched on the chance that a second committer was able to
   * successfully commit on top of our commit.
   *
   * @param tableOrViewName full name of the Table/View
   * @param newMetadataLocation the path of the new commit file
   * @param properties properties for retry
   * @param commitStatusSupplier check if the latest metadata presents or not using metadata
   *     location for table.
   * @return Commit Status of Success, Failure or Unknown
   */
  protected BaseMetastoreOperations.CommitStatus checkCommitStatus(
      String tableOrViewName,
      String newMetadataLocation,
      Map<String, String> properties,
      Supplier<Boolean> commitStatusSupplier) {
    int maxAttempts =
        PropertyUtil.propertyAsInt(
            properties,
            TableProperties.COMMIT_NUM_STATUS_CHECKS,
            TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT);
    long minWaitMs =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS,
            TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT);
    long maxWaitMs =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS,
            TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT);
    long totalRetryMs =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS,
            TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT);

    AtomicReference<BaseMetastoreOperations.CommitStatus> status =
        new AtomicReference<>(BaseMetastoreOperations.CommitStatus.UNKNOWN);

    Tasks.foreach(newMetadataLocation)
        .retry(maxAttempts)
        .suppressFailureWhenFinished()
        .exponentialBackoff(minWaitMs, maxWaitMs, totalRetryMs, 2.0)
        .onFailure(
            (location, checkException) ->
                LOG.error("Cannot check if commit to {} exists.", tableOrViewName, checkException))
        .run(
            location -> {
              boolean commitSuccess = commitStatusSupplier.get();

              if (commitSuccess) {
                LOG.info(
                    "Commit status check: Commit to {} of {} succeeded",
                    tableOrViewName,
                    newMetadataLocation);
                status.set(BaseMetastoreOperations.CommitStatus.SUCCESS);
              } else {
                LOG.warn(
                    "Commit status check: Commit to {} of {} unknown, new metadata location is not current "
                        + "or in history",
                    tableOrViewName,
                    newMetadataLocation);
              }
            });

    if (status.get() == BaseMetastoreOperations.CommitStatus.UNKNOWN) {
      LOG.error(
          "Cannot determine commit state to {}. Failed during checking {} times. "
              + "Treating commit state as unknown.",
          tableOrViewName,
          maxAttempts);
    }
    return status.get();
  }
}
