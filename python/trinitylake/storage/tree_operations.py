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
from const import IPC_FILE_SUFFIX
from trinitylake.tree import TrinityTree
from trinitylake.storage import Storage
from trinitylake.const import LATEST_HINT_FILE
from trinitylake.storage import file_paths


def find_latest_root(storage: Storage) -> TrinityTree:
    latest_version = find_latest_version(storage)
    tree_path = file_paths.get_root_node_path(latest_version)
    root_node = storage.deserialize_tree(tree_path)
    return TrinityTree(root_node)


def find_latest_version(storage: Storage):
    version_hint = 0
    if storage.exists(LATEST_HINT_FILE):
        version_hint = int(storage.read_file(LATEST_HINT_FILE).decode())
        return version_hint
    current_version = version_hint
    while True:
        next_version = current_version + 1
        next_root = file_paths.get_root_node_path(next_version)
        if not storage.exists(next_root):
            break
        current_version = next_version
    return current_version
