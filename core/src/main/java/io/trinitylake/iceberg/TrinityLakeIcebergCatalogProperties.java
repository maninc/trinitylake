/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trinitylake.iceberg;

public class TrinityLakeIcebergCatalogProperties {

  private TrinityLakeIcebergCatalogProperties() {}

  public static final String STORAGE_TYPE = "storage.type";

  public static final String STORAGE_OPS_PROPERTIES_PREFIX = "storage.ops.";

  public static final String VERSION_NAMESPACE_PREFIX = "vn.namespace-prefix";

  public static final String TRANSACTION_NAMESPACE_PREFIX = "txn.namespace-prefix";

  public static final String TRANSACTION_ISOLATION_LEVEL = "txn.isolation-level";

  public static final String TRANSACTION_TTL_MILLIS = "txn.ttl-millis";
}
