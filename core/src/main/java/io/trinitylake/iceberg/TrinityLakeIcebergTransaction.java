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

import io.trinitylake.RunningTransaction;
import io.trinitylake.TrinityLake;
import io.trinitylake.storage.LakehouseStorage;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;

public class TrinityLakeIcebergTransaction implements Transaction {

  private final LakehouseStorage storage;
  private final Table table;
  private final String distTransactionId;

  public TrinityLakeIcebergTransaction(
      LakehouseStorage storage, Table table, String distTransactionId) {
    this.storage = storage;
    this.table = table;
    this.distTransactionId = distTransactionId;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public UpdateSchema updateSchema() {
    return table.updateSchema();
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return table.updateSpec();
  }

  @Override
  public UpdateProperties updateProperties() {
    return table.updateProperties();
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return table.replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation() {
    return table.updateLocation();
  }

  @Override
  public AppendFiles newAppend() {
    return table.newAppend();
  }

  @Override
  public AppendFiles newFastAppend() {
    return table.newFastAppend();
  }

  @Override
  public RewriteFiles newRewrite() {
    return table.newRewrite();
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return table.rewriteManifests();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return table.newOverwrite();
  }

  @Override
  public RowDelta newRowDelta() {
    return table.newRowDelta();
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return table.newReplacePartitions();
  }

  @Override
  public DeleteFiles newDelete() {
    return table.newDelete();
  }

  @Override
  public UpdateStatistics updateStatistics() {
    return table.updateStatistics();
  }

  @Override
  public UpdatePartitionStatistics updatePartitionStatistics() {
    return table.updatePartitionStatistics();
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return table.expireSnapshots();
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return table.manageSnapshots();
  }

  @Override
  public void commitTransaction() {
    RunningTransaction transaction = TrinityLake.loadDistTransaction(storage, distTransactionId);
    TrinityLake.commitTransaction(storage, transaction);
  }
}
