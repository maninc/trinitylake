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

import io.trinitylake.models.LakehouseDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableSet;
import io.trinitylake.util.PropertyUtil;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransactionOptions implements StringMapBased {

  public static final String ISOLATION_LEVEL = "isolation-level";
  public static final String ISOLATION_LEVEL_DEFAULT = "snapshot";
  public static final String TXN_ID = "txn-id";
  public static final String TXN_VALID_MILLIS = "txn-valid-millis";

  public static final Set<String> OPTIONS =
      ImmutableSet.<String>builder().add(ISOLATION_LEVEL).add(TXN_ID).add(TXN_VALID_MILLIS).build();

  private final Map<String, String> options;
  private final IsolationLevel isolationLevel;
  private final String txnId;
  private final long txnValidMillis;

  public TransactionOptions(LakehouseDef lakehouseDef, Map<String, String> options) {
    this.options = PropertyUtil.filterProperties(options, OPTIONS::contains);
    this.isolationLevel =
        IsolationLevel.valueOf(
            PropertyUtil.propertyAsString(options, ISOLATION_LEVEL, ISOLATION_LEVEL_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    this.txnId = PropertyUtil.propertyAsString(options, TXN_ID, UUID.randomUUID().toString());
    this.txnValidMillis =
        PropertyUtil.propertyAsLong(options, TXN_VALID_MILLIS, lakehouseDef.getTxnValidMillis());
  }

  @Override
  public Map<String, String> asStringMap() {
    return options;
  }

  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  public String txnId() {
    return txnId;
  }

  public long txnValidMillis() {
    return txnValidMillis;
  }
}
