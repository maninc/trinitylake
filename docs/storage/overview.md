# Overview

Storage is the most important component for the TrinityLake format.
In general, the TrinityLake format is designed to be heavily cached and
work together with both the local and remote storage.

Each node file in a TrinityLake tree is around 1MB in size by default, allowing each node to be efficiently
cached to local disk. With the use of Arrow IPC format, access to those locally cached files leverages Arrow's memory mapping
that allows for zero-copy reads, where the data is accessed directly from the file on disk without being copied into memory, 
eliminating the need for any serialization or deserialization.

## Lakehouse Storage vs Storage Operations

When using TrinityLake SDKs, it is important to understand the distinction of **LakehouseStorage** vs **StorageOperations** (a.k.a. StorageOps).

A LakehouseStorage describes operations that can be performed against paths after a Lakehouse root.
Due to the nature of TrinityLake's design, it is expected that a LakehouseStorage is able to 
handle the cache-based interaction across remote and local storages. 

A StorageOps describes the operations that can be performed against a specific storage solution like local disk or Amazon S3.
It is implemented without any context of a lakehouse or root location.


## StorageOps Common Properties

Each StorageOps has its own set of configurable properties.
The following properties are common and should be respected by all types of storage ops:

| Property Key                         | Description                                                                    | Default                                         |
|--------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------|
| delete.batch-size                    | Default batch size for each call for batch file deletion                       | 100                                             |
| prepare-read.cache-size              | The cache size for pulling remote storage files to local to prepare for a read | 1000                                            |
| prepare-read.cache-expiration-millis | The duration that a cached file is safe from cache eviction in milliseconds    | 600000 (10 minutes)                             |
| prepare-read.staging-dir             | Staging directory used for read caching                                        | JVM `java.io.tmpdir` configuration for Java SDK |
| write.staging-dir                    | Staging directory used for write                                               | JVM `java.io.tmpdir` configuration for Java SDK |
