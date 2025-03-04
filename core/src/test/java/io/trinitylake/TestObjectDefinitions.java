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

import static org.assertj.core.api.Assertions.assertThat;

import io.trinitylake.models.Column;
import io.trinitylake.models.DataType;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.SQLRepresentation;
import io.trinitylake.models.Schema;
import io.trinitylake.models.TableDef;
import io.trinitylake.models.ViewDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import io.trinitylake.storage.local.LocalStorageOpsProperties;
import java.io.File;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestObjectDefinitions {

  @TempDir private File tempDir;
  private LakehouseStorage storage;

  private final String testNamespaceName = "test-namespace";
  private final String testViewName = "test-view";
  private final String testTableName = "test-table";

  private final LakehouseDef lakehouseDef = ObjectDefinitions.newLakehouseDefBuilder().build();
  private final NamespaceDef namespaceDef = NamespaceDef.newBuilder()
          .putProperties("k1", "v1").build();
  private final TableDef tableDef = TableDef.newBuilder()
          .setSchema(Schema.newBuilder()
                  .addColumns(
                          Column.newBuilder()
                                  .setName("col1")
                                  .setType(DataType.VARCHAR)
                                  .build()))
          .putProperties("k1", "v1")
          .build();

  private final ViewDef testViewDef =
      ObjectDefinitions.newViewDefBuilder()
          .setSchemaBinding(false)
          .addSqlRepresentations(
              SQLRepresentation.newBuilder()
                  .setType("sql")
                  .setSql("select 'foo' foo")
                  .setDialect("spark-sql")
                  .build())
          .putProperties("k1", "v1")
          .build();

  @BeforeEach
  public void beforeEach() {
    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));
    this.storage =
        new BasicLakehouseStorage(
            new LiteralURI("file://" + tempDir),
            new LocalStorageOps(props, LocalStorageOpsProperties.instance()));
  }

  @Test
  public void testWriteViewDef() {
    String testViewDefFilePath = FileLocations.newViewDefFilePath(testNamespaceName, testViewName);
    File testViewDefFile = new File(tempDir, testViewDefFilePath);

    assertThat(testViewDefFile.exists()).isFalse();
    ObjectDefinitions.writeViewDef(
        storage, testViewDefFilePath, testNamespaceName, testViewName, testViewDef);
    assertThat(testViewDefFile.exists()).isTrue();
  }

  @Test
  public void testReadViewDef() {
    String testViewDefFilePath = FileLocations.newViewDefFilePath(testNamespaceName, testViewName);
    File testViewDefFile = new File(tempDir, testViewDefFilePath);

    assertThat(testViewDefFile.exists()).isFalse();
    ObjectDefinitions.writeViewDef(
        storage, testViewDefFilePath, testNamespaceName, testViewName, testViewDef);
    assertThat(testViewDefFile.exists()).isTrue();

    ViewDef viewDef = ObjectDefinitions.readViewDef(storage, testViewDefFilePath);
    assertThat(viewDef).isEqualTo(testViewDef);
  }

  @Test
  public void testWriteNamespaceDef() {
    String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
    File lakehouseDefFile = new File(tempDir, lakehouseDefFilePath);
    assertThat(lakehouseDefFile.exists()).isFalse();
    ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
    assertThat(lakehouseDefFile.exists()).isTrue();
    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(testNamespaceName);

    File namespaceDefFile = new File(tempDir, namespaceDefFilePath);
    assertThat(namespaceDefFile.exists()).isFalse();
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, testNamespaceName, namespaceDef);
    assertThat(namespaceDefFile.exists()).isTrue();
  }

  @Test
  public void testReadNamespaceDef() {
    String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
    ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(testNamespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, testNamespaceName, namespaceDef);

    NamespaceDef testNamespaceDef = ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath);
    assertThat(testNamespaceDef).isEqualTo(namespaceDef);
  }

  @Test
  public void testWriteTableDef() {
    ObjectDefinitions.writeLakehouseDef(storage, FileLocations.newLakehouseDefFilePath(), lakehouseDef);
    ObjectDefinitions.writeNamespaceDef(storage, FileLocations.newNamespaceDefFilePath(testNamespaceName),
            testNamespaceName, namespaceDef);
    String tableDefFilePath = FileLocations.newTableDefFilePath(testNamespaceName, testTableName);
    File tableDefFile = new File(tempDir, tableDefFilePath);

    assertThat(tableDefFile.exists()).isFalse();
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, testNamespaceName, testTableName, tableDef);
    assertThat(tableDefFile.exists()).isTrue();
  }

  @Test
  public void testReadTableDef() {
    ObjectDefinitions.writeLakehouseDef(storage, FileLocations.newLakehouseDefFilePath(), lakehouseDef);
    ObjectDefinitions.writeNamespaceDef(storage, FileLocations.newNamespaceDefFilePath(testNamespaceName),
            testNamespaceName, namespaceDef);
    String tableDefFilePath = FileLocations.newTableDefFilePath(testNamespaceName, testTableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, testNamespaceName, testTableName, tableDef);

    TableDef testTableDef = ObjectDefinitions.readTableDef(storage, tableDefFilePath);
    assertThat(testTableDef).isEqualTo(tableDef);
  }
}
