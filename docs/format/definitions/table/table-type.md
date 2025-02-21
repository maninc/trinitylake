# Table Type

There are 3 table types in TrinityLake, MANAGED, EXTERNAL and FEDERATED.

## MANAGED

A managed table is fully compliant with the [transaction semantics](../storage-transaction) defined by the TrinityLake format.
It can participate in multi-object and multi-statement transactions with any other managed objects in the same Trinity Lakehouse.
When dropping the table, the data is also deleted.

TrinityLake provides the overall semantics of a managed table in areas like [schema](table-schema.md),
[streaming](streaming.md) and [upsert](upsert.md) behaviors, etc. and the behavior can be implemented using various table and file formats
such as [Apache Iceberg with Apache Parquet](./iceberg.md).

## EXTERNAL

An external table is managed by an external system that a Trinity Lakehouse has no knowledge about.
It has 4 key characteristics:

1. External tables are **read-only**, thus do not participate in write transactions.
2. The external table definition is **static** and requires either manual refresh or some sort of pull/push-based mechanism to trigger the refresh.
3. **Schema on read** is possible for user to define a specific read schema that does not need to comply with the underlying data source schema.
4. When dropping the table, merely the table definition is dropped, the source table in the external system remains untouched.

## FEDERATED

A federated table is managed by an external system that a Trinity Lakehouse can connect to 
and perform read or write or both through the federation connection.

Compared to external table, federated table could support more operations such as:

- Writing to the table
- Altering or dropping the source table definition
- Always reading the latest table without the need for manual or push/pull-based refresh

The key characteristics of federated table from ACID perspective is that,
the latest version of the table for read is not determined at transaction start time,
but at the time that the table is initially loaded.
This is similar to the [Read Latest](streaming.md#read-latest) isolation mode that we see in managed table streaming,
but for federated table it is even worse because we cannot make assumptions about how "latest" the response really is,
and we also do not know the implication of writing to such a "latest" table.

Because of this behavior, compared to managed table, federated table lowers the guarantee of the transactional 
semantics provided by the TrinityLake format. If a user performs multi-table transaction against a managed table 
with a federated table, the isolation level would be lowered to READ UNCOMMITTED level in the worst case.
For example, a federated table could be rolling back while a managed table reads its data in a 
JOIN operation, causing a dirty read.

In addition, behaviors for SQL operations like `DROP TABLE` might not be strictly defined.
If the federated system does not follow the standard SQL semantics for deleting data for its managed table,
there is no way for TrinityLake to enforce the strict SQL semantics.

In summary, the FEDERATED table type provides more capabilities and stronger flexibility than EXTERNAL table type,
by trading off the strong SQL transactional ACID guarantees provided by the MANAGED table type 
and should be used with caution.