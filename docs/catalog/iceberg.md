# Iceberg Catalog

TrinityLake provides an implementation of the pluggable `Catalog` API standard in Apache Iceberg.
Currently we provide the Java implementation `io.trinitylake.iceberg.TrinityLakeIcebergCatalog`.
Python and Rust implementations are work in progress.
See related Iceberg documentation for how to use it [standalone](https://iceberg.apache.org/docs/nightly/java-api-quickstart/) or 
with various query engines like [Apache Spark](https://iceberg.apache.org/docs/nightly/spark-configuration/#catalog-configuration), 
[Apache Flink](https://iceberg.apache.org/docs/nightly/flink/#catalog-configuration),
and [Apache Hive](https://iceberg.apache.org/docs/nightly/hive/#custom-iceberg-catalogs).

## Catalog Properties

The TrinityLake Iceberg catalog exposes the following catalog properties:

| Property Name                | Description                                                                                                                 | Required? | Default                                                                       |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-----------|-------------------------------------------------------------------------------|
| warehouse                    | The Iceberg catalog warehouse location. For TrinityLake root URI of the Lakehouse storage                            | Yes       |                                                                               |
| storage.ops.<scheme\>.<key\> | Any property configuration for a specific type of storage operation. See [Storage](../storage/overview.md) for more details. | No        |                                                                               |
| txn.namespace-prefix         | The prefix to indicate a namespace is used to begin a transaction                                                           | No        | txn_                                                                          |
| txn.isolation-level          | The default isolation level for a transaction                                                                               | No        | default setting in [Lakehouse Definition](../format/definitions/lakehouse.md)                                                                      |
| txn.valid-millis             | The default duration for which a transaction is valid in milliseconds                                                       | No        | default setting in [Lakehouse Definition](../format/definitions/lakehouse.md) |

For example, a user can initialize a TrinityLake Iceberg catalog with:

```java
import io.trinitylake.iceberg.TrinityLakeIcebergCatalog;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogProperties;
import org.apache.iceberg.catalog.SupportsNamespaces;

Catalog catalog = new TrinityLakeIcebergCatalog();
catalog.initialize(
        ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION, // "warehouse"
                "s3://my-bucket"));

SupportsNamespaces nsCatalog = (SupportsNamespace) catalog;
```

## Using Distributed Transaction
 
You can use the TrinityLake transaction semantics through Iceberg multi-level namespace.

### Begin a transaction

If you create a namespace with a prefix matching the `txn.namespace-prefix`, then it is considered beginning a transaction.

The namespace properties are used to provide runtime override options for the transaction. The following options are supported:

| Option Name     | Description                                                   |
|-----------------|---------------------------------------------------------------|
| isolation-level | The isolation level of this transaction                       |
| ttl-millis      | The duration for which a transaction is valid in milliseconds |

The act of creating such a namespace means to create a distributed transaction that is persisted in the lakehouse.
For example, consider a user creating a transaction `txn_1`:

```java
import org.apache.iceberg.Namespace;

String txnNamespaceName = "txn_1";
Namesapce txnNamesapce = Namespace.of(txnNamespaceName);

// begin a transaction with SERIALIZABLE level isolation
nsCatalog.createNamespace(
        txnNamespace, 
        ImmutableMap.of("isolation-level", "serializable"));
```

### Using the transaction

After creation, a user can access the specific isolated version of the lakehouse under the namespace.
For example, consider a Trinity Lakehouse with namespace `ns1` and table `t1`,
then the user should see a namespace `txn_1.ns1` and a table `txn_1.ns1.t1` which the user can read and write to:

```java
import static org.assertj.core.api.Assertions.assertThat;

// list namespaces in this transaction
assertThat(catalog.listNamespaces(txnNamespace))
        .containsExactly(Namespace.of(txnNamespaceName, "ns1"));

// list tables in this transaction under namespace ns1
assertThat(catalog.listTables(Namespace.of(txnNamespaceName, "ns1")))
        .containsExactly(TableIdentifier.of(txnNamespaceName, "ns1", "t1"));
```

### Commit a transaction

In order to commit this transaction, set the namesapce property `commit` to `true`:

```java
nsCatalog.setProperties(
        txnNamespace, 
        ImmutbaleMap.of("commit", "true"));
```

### Rollback a transaction

In order to rollback a transaction, perform a drop namespace:

```java
nsCatalog.dropNamespace(txnNamespace);
```