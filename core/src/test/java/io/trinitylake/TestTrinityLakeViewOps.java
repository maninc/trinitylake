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

import io.trinitylake.models.FullName;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.SQLRepresentation;
import io.trinitylake.models.ViewDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import io.trinitylake.storage.local.LocalStorageOpsProperties;
import io.trinitylake.tree.TreeOperations;
import io.trinitylake.tree.TreeRoot;
import java.io.File;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTrinityLakeViewOps {

  private static final LakehouseDef LAKEHOUSE_DEF =
      LakehouseDef.newBuilder()
          .setNamespaceNameMaxSizeBytes(8)
          .setTableNameMaxSizeBytes(8)
          .setViewNameMaxSizeBytes(8)
          .build();
  private static final String NS1 = "ns1";
  private static final NamespaceDef NS1_DEF =
      NamespaceDef.newBuilder().putProperties("k1", "v1").build();

  @TempDir private File tempDir;

  private LakehouseStorage storage;

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

    TrinityLake.createLakehouse(storage, LAKEHOUSE_DEF);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createNamespace(storage, transaction, NS1, NS1_DEF);
    TrinityLake.commitTransaction(storage, transaction);
  }

  @Test
  public void testCreateView() {
    ViewDef viewDef =
        ViewDef.newBuilder()
            .setId("0")
            .setSchemaBinding(false)
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select 'foo' foo")
                    .setDialect("spark-sql")
                    .build())
            .addReferencedObjectFullNames(
                FullName.newBuilder().setNamespaceName(NS1).setName("tb1").build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NS1, "v1", viewDef);
    TrinityLake.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertThat(root.path().isPresent()).isTrue();
    assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(2));
    assertThat(root.previousRootNodeFilePath().isPresent()).isTrue();
    assertThat(root.previousRootNodeFilePath().get()).isEqualTo(FileLocations.rootNodeFilePath(1));
    String v1Key = ObjectKeys.viewKey("ns1", "v1", LAKEHOUSE_DEF);
    Optional<String> v1Path = TreeOperations.searchValue(storage, root, v1Key);
    assertThat(v1Path.isPresent()).isTrue();
    ViewDef readDef = ObjectDefinitions.readViewDef(storage, v1Path.get());
    assertThat(readDef).isEqualTo(viewDef);
  }

  @Test
  public void testCreateDescribeView() {
    ViewDef viewDef =
        ViewDef.newBuilder()
            .setId("0")
            .setSchemaBinding(false)
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select 'foo' foo")
                    .setDialect("spark-sql")
                    .build())
            .addReferencedObjectFullNames(
                FullName.newBuilder().setNamespaceName(NS1).setName("tb1").build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NS1, "v1", viewDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, "ns1", "v1")).isTrue();
    ViewDef v1DefDescribe = TrinityLake.describeView(storage, transaction, "ns1", "v1");
    assertThat(v1DefDescribe).isEqualTo(viewDef);
  }

  @Test
  public void testCreateDescribeReplaceDescribeView() {
    ViewDef viewDef =
        ViewDef.newBuilder()
            .setId("0")
            .setSchemaBinding(false)
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select 'foo' foo")
                    .setDialect("spark-sql")
                    .build())
            .addReferencedObjectFullNames(
                FullName.newBuilder().setNamespaceName(NS1).setName("tb1").build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NS1, "v1", viewDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, "ns1", "v1")).isTrue();
    ViewDef v1DefDescribe = TrinityLake.describeView(storage, transaction, "ns1", "v1");
    assertThat(v1DefDescribe).isEqualTo(viewDef);

    ViewDef v1DefAlter =
        ViewDef.newBuilder()
            .setId("0")
            .setSchemaBinding(false)
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select 'foo' foo")
                    .setDialect("spark-sql")
                    .build())
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select * from foo")
                    .setDialect("spark-sql")
                    .build())
            .addReferencedObjectFullNames(
                FullName.newBuilder().setNamespaceName(NS1).setName("tb1").build())
            .putProperties("k1", "v1")
            .build();
    transaction = TrinityLake.replaceView(storage, transaction, "ns1", "v1", v1DefAlter);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, "ns1", "v1")).isTrue();
    ViewDef v1DefAlterDescribe = TrinityLake.describeView(storage, transaction, "ns1", "v1");
    assertThat(v1DefAlterDescribe).isEqualTo(v1DefAlter);
  }

  @Test
  public void testCreateDescribeDropDescribeNamespace() {
    ViewDef viewDef =
        ViewDef.newBuilder()
            .setId("0")
            .setSchemaBinding(false)
            .addSqlRepresentations(
                SQLRepresentation.newBuilder()
                    .setType("sql")
                    .setSql("select 'foo' foo")
                    .setDialect("spark-sql")
                    .build())
            .addReferencedObjectFullNames(
                FullName.newBuilder().setNamespaceName(NS1).setName("tb1").build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createView(storage, transaction, NS1, "v1", viewDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, "ns1", "v1")).isTrue();
    ViewDef v1DefDescribe = TrinityLake.describeView(storage, transaction, "ns1", "v1");
    assertThat(v1DefDescribe).isEqualTo(viewDef);

    transaction = TrinityLake.dropView(storage, transaction, "ns1", "v1");
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    assertThat(TrinityLake.viewExists(storage, transaction, "ns1", "v1")).isFalse();
  }
}
