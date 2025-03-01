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

import com.google.protobuf.Descriptors;
import io.trinitylake.DropNamespaceBehavior;
import io.trinitylake.ObjectDefinitions;
import io.trinitylake.RunningTransaction;
import io.trinitylake.TransactionOptions;
import io.trinitylake.TrinityLake;
import io.trinitylake.exception.ObjectAlreadyExistsException;
import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableList;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.relocated.com.google.common.collect.Lists;
import io.trinitylake.relocated.com.google.common.collect.Maps;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LakehouseStorages;
import io.trinitylake.util.PropertyUtil;
import io.trinitylake.util.ValidationUtil;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinityLakeIcebergCatalog implements Catalog, SupportsNamespaces {

  private static final Logger LOG = LoggerFactory.getLogger(TrinityLakeIcebergCatalog.class);

  private LakehouseStorage storage;
  private TrinityLakeIcebergCatalogProperties catalogProperties;
  private String catalogName;
  private Map<String, String> allProperties;
  private MetricsReporter metricsReporter;

  /**
   * Constructor for dynamic initialization. It is expected to call {@code initialize} after using
   * this constructor
   */
  public TrinityLakeIcebergCatalog() {}

  /**
   * Constructor for directly initializing a catalog
   *
   * @param name catalog name
   * @param properties catalog properties
   */
  public TrinityLakeIcebergCatalog(String name, Map<String, String> properties) {
    initialize(name, properties);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String warehouse =
        PropertyUtil.propertyAsNullableString(properties, CatalogProperties.WAREHOUSE_LOCATION);

    if (!warehouse.contains("://")) {
      warehouse = "file://" + warehouse;
    }

    String storageType =
        PropertyUtil.propertyAsNullableString(
            properties, TrinityLakeIcebergCatalogProperties.STORAGE_TYPE);
    Map<String, String> storageOpsProperties =
        PropertyUtil.propertiesWithPrefix(
            properties, TrinityLakeIcebergCatalogProperties.STORAGE_OPS_PROPERTIES_PREFIX);

    Map<String, String> storageProperties = Maps.newHashMap();
    storageProperties.putAll(storageOpsProperties);
    if (storageType != null) {
      storageProperties.put(LakehouseStorages.STORAGE_TYPE, storageType);
    }
    storageProperties.put(LakehouseStorages.STORAGE_ROOT, warehouse);

    this.storage = LakehouseStorages.initialize(storageProperties);
    this.catalogProperties = new TrinityLakeIcebergCatalogProperties(properties);
    this.allProperties = ImmutableMap.copyOf(storageOpsProperties);
    this.catalogName = name;
    this.metricsReporter = CatalogUtil.loadMetricsReporter(properties);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    if (namespace.isEmpty()) {
      return false;
    }

    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    if (parseResult.isSystem()) {
      return TrinityLake.lakehouseExists(storage);
    }

    if (parseResult.distTransactionId().isPresent()) {
      return TrinityLake.distTransactionExists(storage, parseResult.distTransactionId().get());
    }

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    return TrinityLake.namespaceExists(storage, transaction, parseResult.namespaceName());
  }

  @Override
  public List<Namespace> listNamespaces() {
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    List<String> namespaces = TrinityLake.showNamespaces(storage, transaction);
    List<Namespace> result =
        Lists.newArrayList(namespaces.stream().map(Namespace::of).collect(Collectors.toList()));
    result.add(Namespace.of(catalogProperties.systemNamespaceName()));
    return result;
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (namespace.isEmpty()) {
      return listNamespaces();
    }

    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    if (parseResult.isSystem()) {
      return ImmutableList.of(
          Namespace.of(
              catalogProperties.systemNamespaceName(),
              catalogProperties.dtxnParentNamespaceName()));
    }

    if (parseResult.distTransactionId().isPresent()) {
      RunningTransaction transaction =
          TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get());
      return TrinityLake.showNamespaces(storage, transaction).stream()
          .map(Namespace::of)
          .collect(Collectors.toList());
    }

    return ImmutableList.of();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    if (parseResult.isSystem()) {
      return ImmutableMap.of();
    }

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);
    try {
      NamespaceDef namespaceDef =
          TrinityLake.describeNamespace(storage, transaction, parseResult.namespaceName());
      return namespaceDef.getPropertiesMap();
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, ImmutableMap.of());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> properties) {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    if (parseResult.isSystem()) {
      LakehouseDef.Builder builder = ObjectDefinitions.newLakehouseDefBuilder();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        Descriptors.FieldDescriptor field =
            builder.getDescriptorForType().findFieldByName(entry.getKey());
        if (field != null) {
          builder.setField(field, entry.getValue());
        }
      }
      TrinityLake.createLakehouse(storage, builder.build());
      return;
    }

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    try {
      transaction =
          TrinityLake.createNamespace(
              storage,
              transaction,
              parseResult.namespaceName(),
              ObjectDefinitions.newNamespaceDefBuilder()
                  .putAllProperties(properties)
                  .setId(UUID.randomUUID().toString())
                  .build());
    } catch (ObjectAlreadyExistsException e) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    if (!parseResult.distTransactionId().isPresent()) {
      TrinityLake.commitTransaction(storage, transaction);
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> propertyKeys)
      throws NoSuchNamespaceException {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    NamespaceDef currentDef;
    try {
      currentDef = TrinityLake.describeNamespace(storage, transaction, parseResult.namespaceName());
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    NamespaceDef.Builder newDefBuilder =
        ObjectDefinitions.newNamespaceDefBuilder().mergeFrom(currentDef);
    propertyKeys.forEach(newDefBuilder::removeProperties);

    transaction =
        TrinityLake.alterNamespace(
            storage, transaction, parseResult.namespaceName(), newDefBuilder.build());

    if (!parseResult.distTransactionId().isPresent()) {
      TrinityLake.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    NamespaceDef currentDef;
    try {
      currentDef = TrinityLake.describeNamespace(storage, transaction, parseResult.namespaceName());
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    NamespaceDef.Builder newDefBuilder =
        ObjectDefinitions.newNamespaceDefBuilder().mergeFrom(currentDef);
    properties.forEach(newDefBuilder::putProperties);

    transaction =
        TrinityLake.alterNamespace(
            storage, transaction, parseResult.namespaceName(), newDefBuilder.build());

    if (!parseResult.distTransactionId().isPresent()) {
      TrinityLake.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);
    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    try {
      transaction =
          TrinityLake.dropNamespace(
              storage, transaction, parseResult.namespaceName(), DropNamespaceBehavior.RESTRICT);
    } catch (ObjectNotFoundException e) {
      LOG.warn("Detected dropping non-existent namespace {}", namespace);
      return false;
    }

    if (!parseResult.distTransactionId().isPresent()) {
      TrinityLake.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    IcebergNamespaceParseResult parseResult =
        IcebergToTrinityLake.parseNamespace(namespace, catalogProperties);

    if (parseResult.isSystem()) {
      return ImmutableList.of();
    }

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);
    List<String> tableNames =
        TrinityLake.showTables(storage, transaction, parseResult.namespaceName());
    return tableNames.stream()
        .map(t -> TableIdentifier.of(namespace, t))
        .collect(Collectors.toList());
  }

  @Override
  public boolean tableExists(TableIdentifier tableIdentifier) {
    IcebergTableIdentifierParseResult parseResult =
        IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    return TrinityLake.tableExists(
        storage, transaction, parseResult.namespaceName(), parseResult.tableName());
  }

  @Override
  public Table loadTable(TableIdentifier tableIdentifier) {
    IcebergTableIdentifierParseResult parseResult =
        IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);
    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    TableOperations ops =
        new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }

    if (parseResult.metadataTableType().isPresent()) {
      return MetadataTableUtils.createMetadataTableInstance(
          ops,
          name(),
          TableIdentifier.of(parseResult.namespaceName(), parseResult.tableName()),
          tableIdentifier,
          parseResult.metadataTableType().get());
    }

    Table table =
        new BaseTable(
            ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);
    LOG.info("Table loaded by catalog: {}", table);
    return table;
  }

  @Override
  public Table registerTable(TableIdentifier tableIdentifier, String metadataFileLocation) {
    IcebergTableIdentifierParseResult parseResult =
        IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);
    ValidationUtil.checkArgument(
        !parseResult.metadataTableType().isPresent(),
        "Cannot register metadata table: %s",
        tableIdentifier);

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);
    try {
      transaction =
          TrinityLake.createTable(
              storage,
              transaction,
              parseResult.namespaceName(),
              parseResult.tableName(),
              ObjectDefinitions.newTableDefBuilder()
                  .setTableFormat(TrinityLakeToIceberg.FORMAT_ICEBERG)
                  .putFormatProperties(
                      TrinityLakeToIceberg.METADATA_LOCATION_FORMAT_PROPERTY, metadataFileLocation)
                  .build());
    } catch (ObjectAlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    commitOrSaveTransaction(parseResult, transaction);
    TableOperations ops =
        new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

    return new BaseTable(
        ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return buildTable(identifier, schema).create();
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return buildTable(identifier, schema).withPartitionSpec(spec).create();
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withProperties(properties)
        .create();
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withLocation(location)
        .withPartitionSpec(spec)
        .withProperties(properties)
        .create();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new TrinityLakeIcebergTableBuilder(identifier, schema);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier) {
    return dropTable(tableIdentifier, true /* drop data and metadata files */);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    // TODO: provide a clear definition for purge vs no purge behavior.
    //  before that we will ignore the purge flag.

    IcebergTableIdentifierParseResult parseResult =
        IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);

    RunningTransaction transaction = beginOrLoadTransaction(parseResult);

    try {
      transaction =
          TrinityLake.dropTable(
              storage, transaction, parseResult.namespaceName(), parseResult.tableName());
      commitOrSaveTransaction(parseResult, transaction);
      return true;
    } catch (ObjectNotFoundException e) {
      return false;
    }
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier identifier, Schema schema, boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties,
      boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(
        identifier, schema, spec, location, properties, orCreate);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return Catalog.super.newCreateTableTransaction(identifier, schema);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec, properties);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec, location, properties);
  }

  @Override
  public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
    // TODO: add support
  }

  @Override
  public void invalidateTable(TableIdentifier identifier) {}

  private RunningTransaction beginOrLoadTransaction(IcebergNamespaceParseResult parseResult) {
    ValidationUtil.checkArgument(
        !parseResult.isSystem(), "Cannot start transaction against system namespace");
    return parseResult.distTransactionId().isPresent()
        ? TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get())
        : TrinityLake.beginTransaction(storage);
  }

  private RunningTransaction beginOrLoadTransaction(IcebergTableIdentifierParseResult parseResult) {
    return parseResult.distTransactionId().isPresent()
        ? TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get())
        : TrinityLake.beginTransaction(storage);
  }

  private void commitOrSaveTransaction(
      IcebergNamespaceParseResult parseResult, RunningTransaction transaction) {
    if (parseResult.distTransactionId().isPresent()) {
      TrinityLake.saveDistTransaction(storage, transaction);
    } else {
      TrinityLake.commitTransaction(storage, transaction);
    }
  }

  private void commitOrSaveTransaction(
      IcebergTableIdentifierParseResult parseResult, RunningTransaction transaction) {
    if (parseResult.distTransactionId().isPresent()) {
      TrinityLake.saveDistTransaction(storage, transaction);
    } else {
      TrinityLake.commitTransaction(storage, transaction);
    }
  }

  private String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return storage.root().extendPath(tableIdentifier.toString()).toString();
  }

  public class TrinityLakeIcebergTableBuilder implements TableBuilder {

    private final TableIdentifier tableIdentifier;
    private final Schema schema;
    private final Map<String, String> tableProperties = Maps.newHashMap();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public TrinityLakeIcebergTableBuilder(TableIdentifier tableIdentifier, Schema schema) {
      this.tableIdentifier = tableIdentifier;
      this.schema = schema;
      this.tableProperties.putAll(tableDefaultProperties());
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      IcebergTableIdentifierParseResult parseResult =
          IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);
      RunningTransaction transaction = beginOrLoadTransaction(parseResult);
      TableOperations ops =
          new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", tableIdentifier);
      }

      return new BaseTable(
          ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);
    }

    @Override
    public Transaction createTransaction() {
      IcebergTableIdentifierParseResult parseResult =
          IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);

      RunningTransaction transaction;
      String distTransactionId;
      if (parseResult.distTransactionId().isPresent()) {
        distTransactionId = parseResult.distTransactionId().get();
        transaction =
            TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get());
      } else {
        distTransactionId = UUID.randomUUID().toString().replace("-", "");
        transaction =
            TrinityLake.beginTransaction(
                storage, ImmutableMap.of(TransactionOptions.ID, distTransactionId));
        parseResult =
            ImmutableIcebergTableIdentifierParseResult.builder()
                .from(parseResult)
                .distTransactionId(distTransactionId)
                .build();
      }

      TableOperations ops =
          new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      ops.commit(null, metadata);
      Table table =
          new BaseTable(
              ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);

      return new TrinityLakeIcebergTransaction(storage, table, distTransactionId);
    }

    @Override
    public Transaction replaceTransaction() {
      IcebergTableIdentifierParseResult parseResult =
          IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);

      RunningTransaction transaction;
      String distTransactionId;
      if (parseResult.distTransactionId().isPresent()) {
        distTransactionId = parseResult.distTransactionId().get();
        transaction =
            TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get());
      } else {
        distTransactionId = UUID.randomUUID().toString().replace("-", "");
        transaction =
            TrinityLake.beginTransaction(
                storage, ImmutableMap.of(TransactionOptions.ID, distTransactionId));
        parseResult =
            ImmutableIcebergTableIdentifierParseResult.builder()
                .from(parseResult)
                .distTransactionId(distTransactionId)
                .build();
      }

      TableOperations ops =
          new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
      ops.commit(ops.current(), metadata);
      Table table =
          new BaseTable(
              ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);

      return new TrinityLakeIcebergTransaction(storage, table, distTransactionId);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      IcebergTableIdentifierParseResult parseResult =
          IcebergToTrinityLake.parseTableIdentifier(tableIdentifier, catalogProperties);

      RunningTransaction transaction;
      String distTransactionId;
      if (parseResult.distTransactionId().isPresent()) {
        distTransactionId = parseResult.distTransactionId().get();
        transaction =
            TrinityLake.loadDistTransaction(storage, parseResult.distTransactionId().get());
      } else {
        distTransactionId = UUID.randomUUID().toString().replace("-", "");
        transaction =
            TrinityLake.beginTransaction(
                storage, ImmutableMap.of(TransactionOptions.ID, distTransactionId));
        parseResult =
            ImmutableIcebergTableIdentifierParseResult.builder()
                .from(parseResult)
                .distTransactionId(distTransactionId)
                .build();
      }

      TableOperations ops =
          new TrinityLakeIcebergTableOperations(storage, allProperties, transaction, parseResult);

      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());

      if (ops.current() == null) {
        TableMetadata metadata =
            TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
        ops.commit(null, metadata);
      } else {
        TableMetadata metadata =
            ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
        ops.commit(ops.current(), metadata);
      }

      Table table =
          new BaseTable(
              ops, TrinityLakeToIceberg.fullTableName(catalogName, parseResult), metricsReporter);

      return new TrinityLakeIcebergTransaction(storage, table, distTransactionId);
    }

    /**
     * Get default table properties set at Catalog level through catalog properties.
     *
     * @return default table properties specified in catalog properties
     */
    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix(
              allProperties, CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    /**
     * Get table properties that are enforced at Catalog level through catalog properties.
     *
     * @return default table properties enforced through catalog properties
     */
    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix(
              allProperties, CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
    }
  }
}
