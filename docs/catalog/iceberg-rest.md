# Iceberg REST Catalog

TrinityLake provides the ability to build an Iceberg REST Catalog (IRC) server 
following the [IRC open catalog standard](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

## Operation Behavior

The TrinityLake-backed IRC offers the same operation behavior as the Iceberg catalog integration.
See [Operation Behavior in Iceberg Catalog](./iceberg.md#operation-behavior) for more details.

## Using System Namespace

The TrinityLake-backed IRC offers the same system namespace support as the Iceberg catalog integration
to perform operations like create lakehouse and list distributed transactions.
See [Using System Namespace in Iceberg Catalog](./iceberg.md#using-system-namespace) for more details.

## Using Distributed Transaction

The TrinityLake-backed IRC offers the same distributed transaction support
as the Iceberg catalog integration using multi-level namespace.
See [Using Distributed Transaction in Iceberg Catalog](./iceberg.md#using-distributed-transaction) for more details.

## Apache Gravitino IRC Server

The easiest way to start a TrinityLake-backed IRC server is to use the Apache Gravitino IRC Server.
You can run the Gravitino Iceberg REST server integrated with TrinityLake backend in two ways.

### Using the Prebuilt Docker Image (Recommended)

Pull the docker image from official trinitylake docker account
```
docker pull trinitylake/trinitylake-gravitino-iceberg-rest-server:latest
```

Run the Gravitino IRC Server container with port mapping
```
docker run -d -p 9001:9001 --name trinitylake-gravitino-iceberg-rest-server trinitylake/trinitylake-gravitino-iceberg-rest-server
```

### Using Apache Gravitino Installation (Manual Setup)

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/how-to-install) 
for downloading and installing the Gravitino software.
After the standard installation process, add the `trinitylake-spark-runtime-3.5_2.12-0.0.1.jar` to your Java classpath or
copy the trinitylake spark runtime jar into Gravitino’s `lib/` directory . 

#### Configuration

Update `gravitino-iceberg-rest-server.conf` with the following configuration:

| Configuration Item                          | Description                                                                                                 | Value                                                  |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| gravitino.iceberg-rest.catalog-backend      | The Catalog backend of the Gravitino Iceberg REST catalog service                                           | custom                                                 |
| gravitino.iceberg-rest.catalog-backend-impl | The Catalog backend implementation of the Gravitino Iceberg REST catalog service.                           | io.trinitylake.iceberg.TrinityLakeIcebergCatalog       |
| gravitino.iceberg-rest.catalog-backend-name | The catalog backend name passed to underlying Iceberg catalog backend.                                      | any name you like, e.g. `trinitylake`                  |
| gravitino.iceberg-rest.uri                  | Iceberg REST catalog server address                                                                         | For local development, use ```http://127.0.0.1:9001``` |
| gravitino.iceberg-rest.warehouse            | Trinity lakehouse storage root URI                                                                          | Any file path. Ex: `/tmp/trinitylake`                  |
| gravitino.iceberg-rest.<key\>               | Any other catalog properties, see [TrinityLake Iceberg catalog properties](./iceberg.md#catalog-properties) |                                                        |

#### Running the server

To start the Gravitino Iceberg REST server
```
./bin/gravitino-iceberg-rest-server.sh start
```

To stop the Gravitino Iceberg REST server
```
./bin/gravitino-iceberg-rest-server.sh stop
```

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/iceberg-rest-service#starting-the-iceberg-rest-server)
for more detailed instructions on starting the IRC server and exploring the namespaces, tables and distributed transactions in the catalog.

### Examples

List catalog configuration settings
```
curl -X GET "http://127.0.0.1:9001/iceberg/v1/config"
```

Create lakehouse
```
curl -X POST "http://127.0.0.1:9001/iceberg/v1/namespaces" \
     -H "Content-Type: application/json" \
     -d '{
           "namespace": ["sys"],
           "properties": {}
         }'
```

List namespaces
```
curl -X GET "http://127.0.0.1:9001/iceberg/v1/namespaces"
```