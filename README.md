# TrinityLake

***An Open Lakehouse Format for Big Data Analytics, ML & AI***

![TrinityLake Logo](https://github.com/trinitylake-io/trinitylake/blob/main/docs/logo/blue-text-horizontal.png?raw=true)

## Introduction

TrinityLake is an **Open Lakehouse Format** for Big Data Analytics, ML & AI.
It defines a storage layout on top of objects like
[Apache Iceberg](https://iceberg.apache.org/) tables,
[Substrait](https://substrait.io/) views, etc.
to form a complete storage-only lakehouse.

It offers the following key features:

- **Storage only** as a lakehouse solution that works exactly the same way locally, on premise and in the cloud
- **Multi-object multi-statement transactions** with standard SQL `BEGIN` and `COMMIT` semantics
- **Consistent time travel and snapshot export** across all objects in the lakehouse
- **Distributed transactions** for complicated write-audit-publish workflows to execute a transaction across multiple engines

For more details about the format and the project community, 
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
