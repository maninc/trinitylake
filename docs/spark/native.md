# Native Connector

TrinityLake provides a native Spark connector using a Spark DataSource V2 (DSv2) catalog implementation for accessing a Trinity lakehouse. 
It also provides Spark SQL extensions for TrinityLake specific SQL operations like transactions.

## Configuration

To configure a TrinityLake Spark DSv2 catalog, you should:

- Add TrinityLake Spark runtime package `io.trinitylake:trinitylake-spark-runtime-3.5_2.12` to the Spark classpath
- Start a Spark session or application using `io.trinitylake.spark.TrinityLakeSparkCatalog` Spark catalog implementation
- Add the TrinityLake Spark SQL extensions `io.trinitylake.spark.TrinityLakeSparkExtensions` to your Spark SQL extensions list
- Set the following Spark catalog configurations:

| Config name          | Description                                                                                                                  | Required? | Default                                                                       |
|----------------------|------------------------------------------------------------------------------------------------------------------------------|-----------|-------------------------------------------------------------------------------|
| storage.root         | The root URI of the TrinityLake storage                                                                                      | Yes       |                                                                               |
| storage.type         | The type of storage                                                                                                          | No        | Inferred from `storage.root` scheme                                           |
| storage.ops.<key\>   | Any property configuration for a specific type of storage operation. See [Storage](../storage/overview.md) for more details. | No        |                                                                               |
| txn.isolation-level  | The default isolation level for a transaction                                                                                | No        | default setting in [Lakehouse Definition](../format/definitions/lakehouse.md) |                                                                     |
| txn.ttl-millis       | The default duration for which a transaction is valid in milliseconds                                                        | No        | default setting in [Lakehouse Definition](../format/definitions/lakehouse.md) |

For example:

```shell
spark-shell \
  --packages io.trinitylake:trinitylake-spark-runtime-3.5_2.12:0.0.1 \
  --conf spark.sql.extensions=io.trinitylake.spark.TrinityLakeSparkExtensions \
  --conf spark.sql.catalog.demo=io.trinitylake.spark.TrinityLakeSparkCatalog \
  --conf spark.sql.catalog.demo.storage.root=s3://my-bucket
```

## SQL Extensions

TrinityLake provides the following Spark SQL extensions for its Spark connector:

### CREATE LAKEHOUSE

Create a new lakehouse at the configured storage root location.

```sql
CREATE LAKEHOUSE [ IF NOT EXISTS ]
       [ LOCATION location ]
       [ LHPROPERTIES ( property_key = property_value [ , ... ] ) ]
```

### BEGIN TRANSACTION

Begin a transaction.

```sql
BEGIN [ TRANSACTION ]
      [ IDENTIFIED BY transaction_id ]
      [ ISOLATION LEVEL { SERIALIZABLE | SNAPSHOT } ]
      [ TXNPROPERTIES ( property_key = property_value [ , ... ] ) ]
```

### COMMIT TRANSACTION

Commit a transaction. 
This command can only be used after executing a [`BEGIN TRANSACTION`](#begin-transaction) or [`LOAD TRANSACTION`](#load-transaction)

```sql
COMMIT [ TRANSACTION ]
```

### SAVE TRANSACTION

Save the current transaction and exit the current transaction context.
This command can only be used after executing a [`BEGIN TRANSACTION`](#begin-transaction) or [`LOAD TRANSACTION`](#load-transaction)

```sql
SAVE [ TRANSACTION ]
```

### LOAD TRANSACTION

Load a transaction of a given transaction ID, and resume its transaction context.

```sql
LOAD [ TRANSACTION ]
     IDENTIFIED BY transaction_id
```

### SET TRANSACTION ISOLATION LEVEL

Set the default isolation level for any new transactions in the session.

```sql
SET [ TRANSACTION ] ISOLATION LEVEL 
    { SERIALIZABLE 
    | SNAPSHOT 
    }
```