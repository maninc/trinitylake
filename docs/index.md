---
title: Home
---

# Welcome to TrinityLake

![logo](./logo/blue-text-horizontal.png)

TrinityLake is an **Open Lakehouse Format** for Big Data Analytics, ML & AI. 
It defines a storage layout on top of objects like
[Apache Iceberg](https://iceberg.apache.org/) tables, 
[Substrait](https://substrait.io/) views, etc.
to form a complete storage-only lakehouse.

## Key Features

### Storage Only

TrinityLake mainly leverages one storage primitive - mutual exclusion of file creation.
This means you can use TrinityLake to build a storage-only lakehouse on 
almost any storage solution including Linux file system,
open source storage solutions like Apache Hadoop Distributed File System (HDFS) or Apache Ozone,
and cloud storage providers like Amazon S3, Google Cloud Storage, Azure Data Lake Storage.

### Multi-Object Multi-Statement Transactions

TrinityLake enables multi-object multi-statement transactions across different tables, indexes, views, 
materialized views, etc. within a lakehouse.
Users could start to leverage standard SQL BEGIN and COMMIT semantics and expect ACID enforcement 
at SNAPSHOT or SERIALIZABLE isolation level across the entire lakehouse.

### Consistent Time Travel, Rollback and Snapshot Export

TrinityLake provides a single timeline for all the transactions that have taken place within a Lakehouse.
Users can perform time travel to get a consistent view of all the objects in the lakehouse,
rollback the lakehouse to a consistent previous state,
or choose to export a snapshot of the entire lakehouse at any given point of time.

### Distributed Transaction for Advanced Write-Audit-Publish

A TrinityLake transaction can be executed not just by a single process, 
but can be distributed around multiple processes.
This could be used for highly complicated write-audit-publish workflows, 
where a writer process can first perform any number of operations against any number of objects in a transactions,
and pass the full transaction to an auditor process to review, modify and eventually commit.

## Integrations

### Open Table Formats

TrinityLake can work with popular open table formats such as Apache Iceberg.
Users can create and use these tables with both the traditional SQL `MANAGED` or `EXTERNAL` experience,
as well as through federation when the table resides in other systems that can be connected to for read and write.
See [Table](./format/definitions/table/overview.md) for more details.

### Open Relational Algebra Formats

TrinityLake can work with open relational algebra formats such as Substrait.
users can use these relational algebra formats to define important objects in a lakehouse
such as view or materialized view.

### Open Catalog Standards

TrinityLake can be used as an implementation of open catalog standards like the Apache Iceberg REST Catalog (IRC) standard.
The project provides catalog servers like an IRC server that users can run as a proxy to access TrinityLake and leverage all open source and 
vendor products that support IRC. This provides a scalable yet lightweight IRC implementation 
where the IRC server is mainly just a proxy, and the main execution logic is pushed down to the storage
layer and handled by this open lakehouse format.
See [Catalog Integration](./catalog/overview.md) for more details.

### Open Engines

Through open table formats and open catalog standards, you can use TrinityLake with any open engine that supports them,
for example with the [Spark Iceberg connector](./spark/iceberg.md).
In addition, TrinityLake provides native connectors to various open engines such as [Apache Spark](./spark/native.md).
These native connectors will provide the full TrinityLake experience to users.