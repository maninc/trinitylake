# Iceberg Connector

TrinityLake can be used through the Spark Iceberg connector by leveraging the 
[TrinityLake Iceberg Catalog](../catalog/iceberg.md) or 
[TrinityLake Iceberg REST Catalog](../catalog/iceberg-rest.md) integrations.

## Configuration

### TrinityLake Iceberg Catalog

To configure an Iceberg catalog with TrinityLake in Spark, you should:

- Add TrinityLake Spark Iceberg runtime package to the Spark classpath
- Use the [Spark Iceberg connector configuration for custom catalog](https://iceberg.apache.org/docs/nightly/spark-configuration/#loading-a-custom-catalog).

For example, to start a Spark shell session with a TrinityLake Iceberg catalog named `demo`:

```shell
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,io.trinitylake:trinitylake-spark-iceberg-runtime-3.5_2.12:0.0.1 \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=io.trinitylake.iceberg.TrinityLakeIcebergCatalog \
  --conf spark.sql.catalog.demo.warehouse=s3://my-bucket
```

### TrinityLake Iceberg REST Catalog

To configure an Iceberg REST catalog with TrinityLake in Spark, you should:

- Start your [TrinityLake IRC server](../catalog/iceberg-rest.md)
- Add TrinityLake Spark Iceberg runtime package to the Spark classpath
- Add TrinityLake Spark extension to the list of Spark SQL extensions `io.trinitylake.spark.iceberg.TrinityLakeIcebergSparkExtensions`
- Use the [Spark Iceberg connector configuration for IRC](https://iceberg.apache.org/docs/nightly/spark-configuration/#catalog-configuration).

For example, to start a Spark shell session with a TrinityLake IRC catalog named `demo` that is running at `http://localhost:8000`:

```shell
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,io.trinitylake:trinitylake-spark-iceberg-runtime-3.5_2.12:0.0.1 \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.type=rest \
  --conf spark.sql.catalog.demo.uri=http://localhost:8000
```

## Using System Namespace

The TrinityLake Spark Iceberg connector offers the same system namespace support
to perform operations like create lakehouse and list distributed transactions.
See [Using System Namespace in Iceberg Catalog](./iceberg.md#using-system-namespace) for more details.

For examples:

```sql
-- create lakehouse
CREATE DATABASE sys;
       
SHOW DATABASES IN sys
---------
|name   |
---------    
|dtxns  |
     
-- list distributed transactions in lakehouse
SHOW DATABASES IN sys.dtxns
------------
|name      |
------------    
|dtxn_123  |
|dtxn_455  |
```

## Using Distributed Transaction

The Spark Iceberg connector for TrinityLake offers the same distributed transaction support using multi-level namespace.
See [Using Distributed Transaction in Iceberg Catalog](./iceberg.md#using-distributed-transaction) for more details.

For examples:

```sql
-- create a transaction with ID 1234
CREATE DATABASE system.dtxns.dtxn_1234
       WITH DBPROPERTIES ('isolation-level'='serializable')
       
-- list tables in transaction of ID 1234 under namespace ns1
SHOW TABLES IN sys.dtxns.txn_1234.ns1;
------
|name|
------     
|t1  |

SELECT * FROM sys.dtxns.txn_1234.ns1.t1;
-----------
|id |data |
-----------
|1  |abc  |
|2  |def  |

INSERT INTO sys.dtxns.txn_1234.ns1.t1 VALUES (3, 'ghi');

-- commit transaction with ID 1234
ALTER DATABASE sys.dtxns.txn_1234
      SET DBPROPERTIES ('commit' = 'true');
```