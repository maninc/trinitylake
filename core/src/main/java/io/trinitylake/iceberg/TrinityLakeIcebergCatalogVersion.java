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

import io.trinitylake.storage.LakehouseStorage;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

public class TrinityLakeIcebergCatalogVersion implements Catalog, SupportsNamespaces {

  private final LakehouseStorage storage;

  public TrinityLakeIcebergCatalogVersion(LakehouseStorage storage) {
    this.storage = storage;
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return SupportsNamespaces.super.namespaceExists(namespace);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return SupportsNamespaces.super.listNamespaces();
  }

  @Override
  public void createNamespace(Namespace namespace) {
    SupportsNamespaces.super.createNamespace(namespace);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> set)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> map)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return Map.of();
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return List.of();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map) {}

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return Catalog.super.buildTable(identifier, schema);
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    return Catalog.super.registerTable(identifier, metadataFileLocation);
  }

  @Override
  public void invalidateTable(TableIdentifier identifier) {
    Catalog.super.invalidateTable(identifier);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier) {
    return Catalog.super.dropTable(identifier);
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return Catalog.super.tableExists(identifier);
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
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return Catalog.super.createTable(identifier, schema);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return Catalog.super.createTable(identifier, schema, spec);
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return Catalog.super.createTable(identifier, schema, spec, properties);
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return Catalog.super.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public String name() {
    return Catalog.super.name();
  }

  @Override
  public Table loadTable(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {}

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
    return false;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return List.of();
  }
}
