# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from trinitylake.protobuf import LakehouseDef
from const import NAMESPACE_SCHEMA_ID_PART, TABLE_SCHEMA_ID_PART, SCHEMA_ID_PART_SIZE


def get_namespace_key(namespace_name: str, lakehouse_def: LakehouseDef) -> str:
    namespace_max_size = lakehouse_def.namespace_name_max_size_bytes
    if len(namespace_name) > namespace_max_size:
        raise ValueError(
            f"namespace name {namespace_name} must be less than or equal to {namespace_max_size}"
            f" in lakehouse definition"
        )

    padded_namespace = namespace_name.ljust(namespace_max_size)
    return f"{NAMESPACE_SCHEMA_ID_PART}{padded_namespace}"


def get_namespace_name_from_key(namespace_key: str) -> str:
    return namespace_key[SCHEMA_ID_PART_SIZE:].strip()


def is_namespace_key(key: str) -> bool:
    return key.startswith(NAMESPACE_SCHEMA_ID_PART)


def get_table_key(
    namespace_name: str, table_name: str, lakehouse_def: LakehouseDef
) -> str:
    namespace_max_size = lakehouse_def.namespace_name_max_size_bytes
    table_max_size = lakehouse_def.table_name_max_size_bytes
    if len(namespace_name) > namespace_max_size:
        raise ValueError(
            f"namespace name {namespace_name} must be less than or equal to {namespace_max_size}"
            f" in lakehouse definition"
        )
    if len(table_name) > table_max_size:
        raise ValueError(
            f"table name {table_name} must be less than or equal to {table_max_size}"
            f" in lakehouse definition"
        )

    padded_table = table_name.ljust(table_max_size)
    return f"{TABLE_SCHEMA_ID_PART}{namespace_name} {padded_table}"


def get_table_name_from_key(namespace_name: str, table_key: str) -> str:
    return table_key[SCHEMA_ID_PART_SIZE + len(namespace_name) :].strip()


def is_table_key(key: str) -> bool:
    return key.startswith(TABLE_SCHEMA_ID_PART)
