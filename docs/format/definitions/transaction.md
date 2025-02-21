# Transaction

A transaction object in a Lakehouse represents a temporary state of the Lakehouse that is not committed yet.
This object is used for distributed transaction where a transaction needs to be passed from one process to another before commit.

## Object Definition Schema

***Schema ID: 4***

| Field Name                    | Protobuf Type | Description                                                                                                                                                                                                                                                    | Required? | Default              |
|-------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------------------|
| id                            | string        | A unique transaction ID                                                                                                                                                                                                                                        | Yes       | auto-generated UUID4 |
| isolation_level               | string        | Isolation level of the transaction                                                                                                                                                                                                                             | Yes       | SNAPSHOT             |
| beginning_root_node_file_path | string        | File path to the beginning root node file path                                                                                                                                                                                                                 | Yes       |                      |
| running_root_node_file_path   | string        | File path to the root node file that represents the current state of the transaction                                                                                                                                                                           | Yes       |                      |
| began_at_millis               | uint64        | The millisecond epoch timestamp for which the transaction began                                                                                                                                                                                                | Yes       |                      |
| expire_at_millis              | uint64        | The millisecond epoch timestamp for which the transaction would expire if not committed. This value must be greater than the `begin_at_millis` and cannot exceed the maximum transaction valid duration setting in the [Lakehouse definition](./lakehouse.md). | Yes       |                      |


