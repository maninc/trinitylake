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
from const import PROTO_BINARY_FILE_SUFFIX, LAKEHOUSE_DEF_FILE_PREFIX, IPC_FILE_SUFFIX


def new_lakehouse_def_path() -> str:
    return f"{LAKEHOUSE_DEF_FILE_PREFIX}{_generate_uuid()}{PROTO_BINARY_FILE_SUFFIX}"


def new_namespace_def_path(namespace_name: str) -> str:
    return f"{LAKEHOUSE_DEF_FILE_PREFIX}{namespace_name}{PROTO_BINARY_FILE_SUFFIX}"


def get_root_node_path(version: int) -> str:
    return f"_{version:032b}{IPC_FILE_SUFFIX}"


def _generate_uuid():
    from uuid import uuid4

    return str(uuid4())
