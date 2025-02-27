# Lakehouse

Lakehouse is the top level container.

## Object Definition Schema

***Schema ID: 0***

| Field Name          | Protobuf Type       | Description                                                                              | Required? | Default            |
|---------------------|---------------------|------------------------------------------------------------------------------------------|-----------|--------------------|
| id                  | string              | A unique UUID of the lakehouse                                                           | Yes       |                    |
| major_version       | uint32              | The major version of the format                                                          | Yes       | 0                  |
| order               | uint32              | The order of the B-epsilon tree                                                          | Yes       | 128                |
| namespace_name_max_size_bytes | uint32              | The maximum size of a namespace name in bytes                                            | Yes       | 100                |
| table_name_max_size_bytes | uint32              | The maximum size of a table name in bytes                                                | Yes       | 100                |
| view_name_max_size_bytes | uint32              | The maximum size of a view name in bytes                                                 | Yes       | 100                |
| node_file_max_size_bytes | uint64              | The maximum size of a node file in bytes                                                 | Yes       | 1048576 (1MB)      |
| properties          | map<string, string> | Free form user-defined key-value string properties                                       | Yes       |                    |
| txn_ttl_millis      | uint64              | The default maximum time duration that a transaction is valid after began in millisecond | Yes       | 604800000 (7 days) |
| txn_isolation_level | IsolationLevel      | The default isolation level of a transaction                                             | Yes       | SNAPSHOT           |

!!!Note

    An update to some of the fields would entail a potentially expensive change of the TrinityLake tree.
    For example, changing the maximum object size or file size would entail re-encode all the keys in the tree.
