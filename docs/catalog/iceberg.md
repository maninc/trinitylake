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

| Property Name       | Description                                                                                                                             | Required? | Default |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------|---------|
| warehouse           | The Iceberg catalog warehouse location, should be set to the root URI for the lakehouse storage                                         | Yes       |         |
| storage.type        | Type of storage. If not set, the type is inferred from the root URI scheme.                                                             | No        |         |
| storage.ops.<key\>  | Any property configuration for a specific type of storage operation. See [Storage](../storage/overview.md) for more details.            | No        |         |
| system.ns-name      | Name of the system namespace                                                                                                            | No        | sys     |
| dtxn.parent-ns-name | Name of the parent namespace name, this parent namespace is within the system namespace and hold all distributed transaction namespaces | No        | dtxns   |
| dtxn.ns-prefix      | The prefix for a namespace to represent a distributed transaction                                                                       | No        | dtxn_   |

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

## Operation Behavior

When using TrinityLake through Iceberg catalog, all the operations will first begin a transaction and then perform the operation.
If the operation modifies the object, it will commit the transaction to the lakehouse.

When using Iceberg transactions to perform multiple operations,
TrinityLake will begin a transaction, perform all the operations and then commit the transaction to the lakehouse.
For example:

```java
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
        
Table table = catalog.loadTable(TableIdentifier.of("ns1", "t1"));
Transaction txn = table.newTransaction();

txn.updateSchema()
    .addColumn("region", Types.StringType.get())
    .commit();

txn.updateSpec()
    .addField("region")
    .commit();

txn.commitTransaction();
```

In this sequence, a TrinityLake transaction will hold the schema and partition spec update, 
and commit both changes to lakehouse in a single transaction. 

## Using System Namespace

The system namespace is a special namespace with name determined by `system.ns-name` that exists 
if a Trinity lakehouse is initialized at the `warehouse` location.
It contains information about the distributed transactions that are available to use in the lakehouse.

### Create a new lakehouse

If there is no Trinity lakehouse initialized at the `warehouse` location, the system namespace will not exist.
The act of creating this system namespace represents creating the lakehouse.
At creation time, [lakehouse definition fields](../format/definitions/lakehouse.md) can be supplied through the namespace properties.

For example:

```java
import org.apache.iceberg.Namespace;

nsCatalog.createNamespace(
        Namespace.of("sys"), 
        ImmutableMap.of("namespace_name_max_size_bytes", "256"));
```

## Using Distributed Transaction
 
You can use the TrinityLake transaction semantics through Iceberg multi-level namespace.

### Distributed transactions namespace

Under the system namespace, there is always a namespace with name determined by `dtxn.parent-ns-name`.
This is the parent namespace that holds all the distributed transactions.
Each distributed transaction is represented by a namespace with prefix determined by `dtxn.ns-prefix`,
followed by the transaction ID.

For example, if there are 2 distributed transactions with IDs `123` and `456`, 
you should see the following namespace hierarchy:

```
└── sys
    └── dtxns
        ├── dtxn_123
        └── dtxn_456
```

### List all transactions

Users can list all the distributed transactions currently in the lakehouse
by doing a namespace listing of the parent namespace of all distributed transactions:

```java
nsCatalog.listNamespace(Namespace.of("sys", "dtxns"));
```

### Begin a transaction

If you create a namespace with a prefix matching the `dtxn.ns-prefix`,
and the namespace is within the system namespace, and also under the parent namespace `dtx.parent-ns-prefix`,
then it is considered as beginning a distributed transaction.

The namespace properties are used to provide runtime override options for the transaction. The following options are supported:

| Option Name     | Description                                                   |
|-----------------|---------------------------------------------------------------|
| isolation-level | The isolation level of this transaction                       |
| ttl-millis      | The duration for which a transaction is valid in milliseconds |

The act of creating such a namespace means to create a distributed transaction that is persisted in the lakehouse.
For example, consider a user creating a transaction with ID `123` with isolation level as `SERIALIZABLE`:

```java
nsCatalog.createNamespace(
        Namespace.of("sys", "dtxns", "dtxn_123"), 
        ImmutableMap.of("isolation-level", "serializable"));
```

### Using the transaction

After creation, a user can access the specific isolated version of the lakehouse under the namespace.
For example, consider a Trinity Lakehouse with namespace `ns1` and table `t1`,
then the user should see a namespace `sys.dtxns.dtxn_123.ns1` 
and a table `sys.dtxns.dtxn_123.ns1.t1` which the user can read and write to:

```java
assertThat(catalog.listNamespaces(Namespace.of("sys", "dtxns", "dtxn_123")))
        .containsExactly(Namespace.of("sys", "dtxns", "dtxn_123", "ns1"));

assertThat(catalog.listTables(Namespace.of("sys", "dtxns", "dtxn_123", "ns1")))
        .containsExactly(TableIdentifier.of("sys", "dtxns", "dtxn_123", "ns1", "t1"));
```

### Commit a transaction

In order to commit this transaction, set the namesapce property `commit` to `true`:

```java
nsCatalog.setProperties(
        Namespace.of("sys", "dtxns", "dtxn_123"), 
        ImmutbaleMap.of("commit", "true"));
```

### Rollback a transaction

In order to rollback a transaction, perform a drop namespace:

```java
nsCatalog.dropNamespace(
        Namespace.of("sys", "dtxns", "dtxn_123"));
```