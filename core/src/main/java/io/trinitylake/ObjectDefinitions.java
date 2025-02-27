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
package io.trinitylake;

import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.models.IsolationLevel;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.models.TransactionDef;
import io.trinitylake.models.ViewDef;
import io.trinitylake.storage.LakehouseStorage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ObjectDefinitions {

  public static final int LAKEHOUSE_MAJOR_VERSION_DEFAULT = 0;

  public static final int LAKEHOUSE_ORDER_DEFAULT = 128;

  public static final int LAKEHOUSE_NAMESPACE_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final int LAKEHOUSE_TABLE_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final int LAKEHOUSE_VIEW_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final long LAKEHOUSE_NODE_FILE_MAX_SIZE_BYTES_DEFAULT = 1048576;

  public static final IsolationLevel LAKEHOUSE_TRANSACTION_ISOLATION_LEVEL_DEFAULT =
      IsolationLevel.SNAPSHOT;

  public static final long LAKEHOUSE_TRANSACTION_TTL_MILLIS_DEFAULT = TimeUnit.DAYS.toMillis(3);

  private ObjectDefinitions() {}

  public static void writeLakehouseDef(
      LakehouseStorage storage, String path, LakehouseDef lakehouseDef) {

    try (OutputStream stream = storage.startCommit(path)) {
      lakehouseDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e, "Failed to write lakehouse definition to storage path %s at %s", path, storage.root());
    }
  }

  public static LakehouseDef readLakehouseDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return LakehouseDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read lakehouse definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static LakehouseDef.Builder newLakehouseDefBuilder() {
    return LakehouseDef.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setMajorVersion(LAKEHOUSE_MAJOR_VERSION_DEFAULT)
        .setOrder(LAKEHOUSE_ORDER_DEFAULT)
        .setNamespaceNameMaxSizeBytes(LAKEHOUSE_NAMESPACE_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setTableNameMaxSizeBytes(LAKEHOUSE_TABLE_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setViewNameMaxSizeBytes(LAKEHOUSE_VIEW_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setNodeFileMaxSizeBytes(LAKEHOUSE_NODE_FILE_MAX_SIZE_BYTES_DEFAULT)
        .setTxnIsolationLevel(LAKEHOUSE_TRANSACTION_ISOLATION_LEVEL_DEFAULT)
        .setTxnTtlMillis(LAKEHOUSE_TRANSACTION_TTL_MILLIS_DEFAULT);
  }

  public static void writeNamespaceDef(
      LakehouseStorage storage, String path, String namespaceName, NamespaceDef namespaceDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      namespaceDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s definition to storage path %s at %s",
          namespaceName,
          path,
          storage.root());
    }
  }

  public static NamespaceDef readNamespaceDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return NamespaceDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read namespace definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static NamespaceDef.Builder newNamespaceDefBuilder() {
    return NamespaceDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static void writeTableDef(
      LakehouseStorage storage,
      String path,
      String namespaceName,
      String tableName,
      TableDef tableDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      tableDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s table %s definition to storage path %s at %s",
          namespaceName,
          tableName,
          path,
          storage.root());
    }
  }

  public static TableDef.Builder newTableDefBuilder() {
    return TableDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static TableDef readTableDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return TableDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read table definition from storage path %s at %s", path, storage.root());
    }
  }

  public static void writeViewDef(
      LakehouseStorage storage,
      String path,
      String namespaceName,
      String viewName,
      ViewDef viewDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      viewDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s view %s definition to storage path %s at %s",
          namespaceName,
          viewName,
          path,
          storage.root());
    }
  }

  public static ViewDef readViewDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return ViewDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read view definition from storage path %s at %s", path, storage.root());
    }
  }

  public static ViewDef.Builder newViewDefBuilder() {
    return ViewDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static void writeTransactionDef(
      LakehouseStorage storage, String path, TransactionDef transactionDef) {
    // TODO: The transaction definition has a fixed path name.
    //  New changes to the transaction state can overwrite the old one.
    //  Ideally we should also use the commit mechanism with a version number for each change.
    //  we will do that in later iterations.
    try (OutputStream stream = storage.startOverwrite(path)) {
      transactionDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write transaction %s definition to storage path %s at %s",
          transactionDef.getId(),
          path,
          storage.root());
    }
  }

  public static TransactionDef readTransactionDef(LakehouseStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return TransactionDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read transaction definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static TransactionDef.Builder newTransactionDefBuilder() {
    return TransactionDef.newBuilder();
  }
}
