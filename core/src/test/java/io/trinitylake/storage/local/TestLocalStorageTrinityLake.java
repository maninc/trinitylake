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
package io.trinitylake.storage.local;

import io.trinitylake.TrinityLake;
import io.trinitylake.TrinityLakeTests;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import java.io.File;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalStorageTrinityLake extends TrinityLakeTests {

  @TempDir private File tempDir;

  private LakehouseStorage storage;

  @BeforeEach
  public void beforeEach() {
    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));

    this.storage =
        new BasicLakehouseStorage(
            new LiteralURI("file://" + tempDir),
            new LocalStorageOps(props, LocalStorageOpsProperties.instance()));

    TrinityLake.createLakehouse(storage, LAKEHOUSE_DEF);
  }

  @Override
  protected LakehouseStorage storage() {
    return storage;
  }
}
