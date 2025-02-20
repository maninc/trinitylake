# Overview

The TrinityLake format currently supports the following objects:

- [Lakehouse](lakehouse.md)
- [Namespace](namespace.md)
- [Table](table/overview.md)
- [View](view.md)
- [Transaction](./transaction.md)

## Schema

Each type of object definition has a different schema, which is defined using [protobuf](https://protobuf.dev/).
The schema should evolve in a way that is backward and froward compatible following the [versioning semantics](../versioning.md#versioning-semantics).

Each schema has a schema ID, and is used as a part of the TrinityLake tree [key encoding](../key-encoding.md#encoded-object-definition-schema-id).

## Traits

Each type of object could have different traits. Currently TrinityLake objects could have the following traits:

| Trait Name | Description                                                                                                  | Objects     |
|------------|--------------------------------------------------------------------------------------------------------------|-------------|
| Assignable | Another object can be assigned to belong to this object                                                      | Namespace   |
| Tabular    | An object that presents data in the form of multiple rows where each row contains the same number of columns | Table, View |


## File Format

The exact definition of each object is serialized into protobuf streams binary files, suffixed with `.binpb`.
These files are called **Object Definition Files (ODF)**.
