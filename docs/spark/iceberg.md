# Iceberg Connector

TrinityLake can be used through the Spark Iceberg connector by leveraging the 
[TrinityLake Iceberg Catalog](../catalog/iceberg.md) or 
[TrinityLake Iceberg REST Catalog](../catalog/iceberg-rest.md) integrations.

## Configuration

### TrinityLake Iceberg Catalog

To configure an Iceberg catalog with TrinityLake in Spark, you should:

- Add TrinityLake Spark Iceberg runtime package to the Spark classpath
- Add TrinityLake Spark extension to the list of Spark SQL extensions `io.trinitylake.spark.iceberg.TrinityLakeIcebergSparkExtensions`
- Use the [Spark Iceberg connector configuration for custom catalog](https://iceberg.apache.org/docs/nightly/spark-configuration/#loading-a-custom-catalog).

For example, to start a Spark shell session with a TrinityLake Iceberg catalog named `demo`:

```shell
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,io.trinitylake:trinitylake-spark-iceberg-runtime-3.5_2.12:0.0.1 \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions,io.trinitylake.spark.iceberg.TrinityLakeIcebergSparkExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=io.trinitylake.iceberg.TrinityLakeIcebergCatalog \
  --conf spark.sql.catalog.demo.warehouse=s3://my-bucket
```

### TrinityLake Iceberg REST Catalog

To configure an Iceberg REST catalog with TrinityLake in Spark, you should:

- Start your TrinityLake IRC server such as the [Apache Gravitino IRC server](../catalog/iceberg-rest.md#apache-gravitino-irc-server)
- Add TrinityLake Spark Iceberg runtime package to the Spark classpath
- Add TrinityLake Spark extension to the list of Spark SQL extensions `io.trinitylake.spark.iceberg.TrinityLakeIcebergSparkExtensions`
- Use the [Spark Iceberg connector configuration for IRC](https://iceberg.apache.org/docs/nightly/spark-configuration/#catalog-configuration).

For example, to start a Spark shell session with a TrinityLake IRC catalog named `demo` that is running at `http://localhost:8000`:

```shell
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,io.trinitylake:trinitylake-spark-iceberg-runtime-3.5_2.12:0.0.1 \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions,io.trinitylake.spark.iceberg.TrinityLakeIcebergSparkExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.type=rest \
  --conf spark.sql.catalog.demo.uri=http://localhost:8000
```

## SQL Extensions

TrinityLake provides the following Spark SQL extensions for a Spark Iceberg connector:

### BEGIN

Begin a transaction.

```sql
BEGIN [ TRANSACTION ]
      [ IDENTIFIED BY transaction_id ]
      [ ISOLATION LEVEL { SERIALIZABLE | SNAPSHOT } ]
      [ OPTIONS ( property_key = property_value [ , ... ] ) ]
```


### COMMIT

Commit a transaction. This command can only be used after executing a [`BEGIN`](#begin) or [`LOAD`](#load)

```sql
COMMIT [ TRANSACTION ]
```


### SAVE

Save the current transaction and exit the current transaction context.

```sql
SAVE [ TRANSACTION ]
```

### LOAD

Load a transaction of a given ID, and resume its transaction context.

```sql
LOAD [ TRANSACTION ]
     [ IDENTIFIED BY transaction_id ]
```

### SET ISOLATION LEVEL

Set the default isolation level for any new transactions in the session.

```sql
SET [ TRANSACTION ] ISOLATION LEVEL 
    { SERIALIZABLE 
    | SNAPSHOT 
    }
```