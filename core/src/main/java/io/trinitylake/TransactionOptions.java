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
package io.trinitylake;

import io.trinitylake.models.IsolationLevel;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableSet;
import io.trinitylake.util.PropertyUtil;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransactionOptions implements StringMapBased {

  public static final String ISOLATION_LEVEL = "isolation-level";
  public static final String ID = "id";
  public static final String TTL_MILLIS = "ttl-millis";

  public static final Set<String> OPTIONS =
      ImmutableSet.<String>builder().add(ISOLATION_LEVEL).add(ID).add(TTL_MILLIS).build();

  private final Map<String, String> options;
  private final IsolationLevel isolationLevel;
  private final String id;
  private final long ttlMillis;

  public TransactionOptions(LakehouseDef lakehouseDef, Map<String, String> options) {
    this.options = PropertyUtil.filterProperties(options, OPTIONS::contains);
    this.isolationLevel =
        options.containsKey(ISOLATION_LEVEL)
            ? IsolationLevel.valueOf(options.get(ISOLATION_LEVEL).toUpperCase(Locale.ENGLISH))
            : lakehouseDef.getTxnIsolationLevel();
    // auto generate UUID if not specified
    this.id = PropertyUtil.propertyAsString(options, ID, UUID.randomUUID().toString());
    this.ttlMillis =
        PropertyUtil.propertyAsLong(options, TTL_MILLIS, lakehouseDef.getTxnTtlMillis());
  }

  @Override
  public Map<String, String> asStringMap() {
    return options;
  }

  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  public String txnId() {
    return id;
  }

  public long txnValidMillis() {
    return ttlMillis;
  }
}
