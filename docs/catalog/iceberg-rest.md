# Iceberg REST Catalog

TrinityLake provides the ability to build an Iceberg REST Catalog (IRC) server 
following the [IRC open catalog standard](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

## Using System Namespace

The TrinityLake-backed IRC offers the same system namespace support 
to perform operations like create lakehouse and list distributed transactions.
See [Using System Namespace in Iceberg Catalog](./iceberg.md#using-system-namespace) for more details.

## Using Distributed Transaction

The TrinityLake-backed IRC offers the same distributed transaction support using multi-level namespace.
See [Using Distributed Transaction in Iceberg Catalog](./iceberg.md#using-distributed-transaction) for more details.

## Apache Gravitino IRC Server

The easiest way to start a TrinityLake-backed IRC server is to use the Apache Gravitino IRC Server.

### Installation

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/how-to-install) 
for downloading and installing the Gravitino software.
After the standard installation process, add the TrinityLake SDK to your Java classpath. 

### Configuration

Use the following configuration to start the Gravitino IRC server:

| Configuration Item                          | Description                                                                                                 | Value                                            |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------|--------------------------------------------------|
| gravitino.iceberg-rest.catalog-backend-impl | The Catalog backend implementation of the Gravitino Iceberg REST catalog service.                           | io.trinitylake.iceberg.TrinityLakeIcebergCatalog |
| gravitino.iceberg-rest.catalog-backend-name | The catalog backend name passed to underlying Iceberg catalog backend.                                      | any name you like, e.g. `trinitylake`            |
| gravitino.iceberg-rest.<key\>               | Any other catalog properties, see [TrinityLake Iceberg catalog properties](./iceberg.md#catalog-properties) |                                                  |

### Running the server

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/iceberg-rest-service#starting-the-iceberg-rest-server)
for starting the IRC server and exploring the namespaces, tables and distributed transactions in the catalog.


