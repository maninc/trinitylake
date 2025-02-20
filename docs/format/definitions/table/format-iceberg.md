# Apache Iceberg Table Format

Apache Iceberg is one of the supported formats of a TrinityLake table.

## Data Type Mapping

TrinityLake maps its data type system to Iceberg in the following way:

| TrinityLake Type | Iceberg Type  |
|------------------|---------------|
| boolean          | boolean       |
| int2             |               |
| int4             | integer       |
| int8             | long          |
| decimal(p, s)    | decimal(p, s) |
| float4           | float         |
| float8           | double        |
| char(n)          |               |
| varchar(n)       | string        |
| date             | date          |
| time3            |               |
| time6            | time          |
| time9            |               |
| timetz3          |               |
| timetz6          |               |
| timetz9          |               |
| timestamp3       |               |
| timestamp6       | timestamp     |
| timestamp9       |               |
| timestamptz3     |               |
| timestamptz6     | timestamptz   |
| timestamptz9     |               |
| fixed(n)         | fixed(n)      |
| varbyte(n)       | binary        |
| struct           | struct        |
| map              | map           |
| list             | list          |

## Managed Iceberg Table

TrinityLake managed Iceberg tables should be created without any format properties in the [table definition](overview.md#object-definition-schema).
The TrinityLake format determines what works the best for managing an Iceberg table within a Trinity Lakehouse.

## External Iceberg Table

To use an external Iceberg table in TrinityLake, you can configure the following [format properties](overview.md#object-definition-schema):

| Property          | Description                                                                                          | Required? | Default |
|-------------------|------------------------------------------------------------------------------------------------------|-----------|---------|
| metadata_location | The location of the Iceberg metadata file                                                            | Yes       |         |
| schema_on_read    | If the table is schema on read. If true, a schema must be provided as a part of the table definition | No        | false   |

## Federated Iceberg Table

To use a federated Iceberg table in TrinityLake, you need to configure Iceberg 
[catalog properties](https://iceberg.apache.org/docs/latest/configuration/?h=catalog+properties#catalog-properties)
inside the [format properties](overview.md#object-definition-schema).
TrinityLake will use the catalog properties to initialize an Iceberg catalog to federate into the external system
to perform read and write.
The federated table's data types will be surfaced to TrinityLake in the same way as external tables.
