# TrinityLake

***An Open Lakehouse Format for Big Data Analytics, ML & AI***

![TrinityLake Logo](https://github.com/trinitylake-io/trinitylake/blob/main/docs/logo/blue-text-horizontal.png?raw=true)

## Introduction

TrinityLake is an **Open Lakehouse Format** for Big Data Analytics, ML & AI.
It allows anyone to build a fully functional lakehouse with storage (e.g. Amazon S3) as the only dependency.

The TrinityLake format defines different objects in a lakehouse and 
provides a consistent and efficient way for accessing and manipulating the interactions among these objects.
It offers the following key features:

- **Storage only** as a lakehouse solution that works exactly the same way locally, on premise and in the cloud
- **Multi-object multi-statement transactions** with standard SQL `BEGIN` and `COMMIT` semantics
- **Consistent time travel and snapshot export** across all objects in the Lakehouse
- **Distributed transactions** which can enable use cases like complicated write-audit-publish workflows
- **Compatibility with open table formats** like Apache Iceberg, supporting both standard SQL `MANAGED` and `EXTERNAL` as well as federation-based access patterns.
- **Compatibility with open catalog standards** like Apache Iceberg REST Catalog specification, serving as a highly scalable yet extremely lightweight backend implementation

For more details about the format, how to get started and how to join our community, 
please visit [trinitylake.io](https://trinitylake.io).

## Building

### Java SDK

TrinityLake Java SDK is built using Gradle with Java 11, 17, 21, or 23.

* Build and run tests: `./gradlew build`
* Build without running tests: `./gradlew build -x test -x integrationTest`
* Fix code style and formatting: `./gradlew spotlessApply`

### Project Website

The TrinityLake project website is built using the [mkdocs-material](https://pypi.org/project/mkdocs-material/) framework with a few other plugins.

#### First time setup

```bash
python3 -m venv env
source env/bin/activate
pip install mkdocs-material
pip install mkdocs-awesome-pages-plugin
```

#### Serve website

```bash
source env/bin/activate
mkdocs serve
```
