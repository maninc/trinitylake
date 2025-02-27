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

  @Override
  public void commitTransaction() {}

  @Override
  public ExpireSnapshots expireSnapshots() {
    return null;
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return Transaction.super.manageSnapshots();
  }

  @Override
  public AppendFiles newAppend() {
    return null;
  }

  @Override
  public DeleteFiles newDelete() {
    return null;
  }

  @Override
  public AppendFiles newFastAppend() {
    return Transaction.super.newFastAppend();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return null;
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return null;
  }

  @Override
  public RewriteFiles newRewrite() {
    return null;
  }

  @Override
  public RowDelta newRowDelta() {
    return null;
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return null;
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return null;
  }

  @Override
  public Table table() {
    return null;
  }

  @Override
  public UpdateLocation updateLocation() {
    return null;
  }

  @Override
  public UpdatePartitionStatistics updatePartitionStatistics() {
    return Transaction.super.updatePartitionStatistics();
  }

  @Override
  public UpdateProperties updateProperties() {
    return null;
  }

  @Override
  public UpdateSchema updateSchema() {
    return null;
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return null;
  }

  @Override
  public UpdateStatistics updateStatistics() {
    return Transaction.super.updateStatistics();
  }
}
