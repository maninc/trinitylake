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

import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.relocated.com.google.common.collect.Maps;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Disabled("Skip until all fixes are done")
class TestTrinityLakeIcebergCatalog extends CatalogTests<TrinityLakeIcebergCatalog> {

  @TempDir private Path warehouse;
  private TrinityLakeIcebergCatalog catalog;

  @BeforeEach
  public void before() {
    catalog =
        initCatalog(
            "trinitylake",
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString()));
    catalog.createNamespace(
        Namespace.of(TrinityLakeIcebergCatalogProperties.SYSTEM_NAMESPACE_NAME_DEFAULT));
  }

  @Override
  protected TrinityLakeIcebergCatalog catalog() {
    return catalog;
  }

  @Override
  protected TrinityLakeIcebergCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    properties.putAll(additionalProperties);
    return new TrinityLakeIcebergCatalog("trinitylake", properties);
  }

  @Override
  protected boolean supportsEmptyNamespace() {
    return false;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Test
  @Override
  public void testRenameTable() {
    // TODO: support rename
  }

  @Test
  @Override
  public void testRenameTableDestinationTableAlreadyExists() {
    // TODO: support rename
  }

  @Test
  @Override
  public void testRenameTableMissingSourceTable() {
    // TODO: support rename
  }

  @Test
  @Override
  public void renameTableNamespaceMissing() {
    // TODO: support rename
  }

  @Test
  @Override
  public void testListTables() {
    super.testListTables();
  }
}
