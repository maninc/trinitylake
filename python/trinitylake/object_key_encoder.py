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
class ObjectKeyEncoder:
    # TODO: follow spec here for object key encoding
    @staticmethod
    def encode_name(name: str, max_size: int) -> str:
        encoded = name.encode("utf-8")
        if len(encoded) > max_size:
            raise ValueError(f"Name exceeds maximum size: {name}")
        return encoded.decode("utf-8")

    @staticmethod
    def encode_schema_id(schema_id: int) -> str:
        from base64 import b64encode

        schema_bytes = schema_id.to_bytes(4, byteorder="big")
        return b64encode(schema_bytes).decode("utf-8")
