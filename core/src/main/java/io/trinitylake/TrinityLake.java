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

import io.trinitylake.exception.CommitFailureException;
import io.trinitylake.exception.ObjectAlreadyExistsException;
import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.models.TransactionDef;
import io.trinitylake.models.ViewDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.tree.BasicTreeRoot;
import io.trinitylake.tree.NodeKeyTableRow;
import io.trinitylake.tree.TreeOperations;
import io.trinitylake.tree.TreeRoot;
import io.trinitylake.util.ValidationUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TrinityLake {

  private TrinityLake() {}

  public static void createLakehouse(LakehouseStorage storage, LakehouseDef lakehouseDef) {
    String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
    ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);

    BasicTreeRoot root = new BasicTreeRoot();
    root.setLakehouseDefFilePath(lakehouseDefFilePath);
    String rootNodeFilePath = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, rootNodeFilePath, root);
    TreeOperations.tryWriteRootNodeVersionHintFile(storage, 0);
  }

  public static RunningTransaction beginTransaction(LakehouseStorage storage) {
    return beginTransaction(storage, ImmutableMap.of());
  }

  public static RunningTransaction beginTransaction(
      LakehouseStorage storage, Map<String, String> options) {
    TreeRoot current = TreeOperations.findLatestRoot(storage);
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, current);
    TransactionOptions transactionOptions = new TransactionOptions(lakehouseDef, options);
    long currentTimeMillis = System.currentTimeMillis();
    return ImmutableRunningTransaction.builder()
        .beganAtMillis(currentTimeMillis)
        .expireAtMillis(currentTimeMillis + transactionOptions.txnValidMillis())
        .transactionId(transactionOptions.txnId())
        .beginningRoot(current)
        .runningRoot(current)
        .isolationLevel(transactionOptions.isolationLevel())
        .build();
  }

  public static CommittedTransaction commitTransaction(
      LakehouseStorage storage, RunningTransaction transaction) throws CommitFailureException {
    ValidationUtil.checkArgument(
        !transaction.runningRoot().path().isPresent(), "There is no change to be committed");
    ValidationUtil.checkState(
        transaction.beginningRoot().path().isPresent(),
        "Cannot find persisted storage path for beginning root");

    String beginningRootNodeFilePath = transaction.beginningRoot().path().get();
    long beginningRootVersion = FileLocations.versionFromNodeFilePath(beginningRootNodeFilePath);
    long nextRootVersion = beginningRootVersion + 1;
    String nextVersionFilePath = FileLocations.rootNodeFilePath(nextRootVersion);
    transaction.runningRoot().setPreviousRootNodeFilePath(beginningRootNodeFilePath);

    TreeOperations.writeRootNodeFile(storage, nextVersionFilePath, transaction.runningRoot());
    TreeOperations.tryWriteRootNodeVersionHintFile(storage, nextRootVersion);
    transaction.runningRoot().setPath(nextVersionFilePath);
    return ImmutableCommittedTransaction.builder()
        .committedRoot(transaction.runningRoot())
        .transactionId(transaction.transactionId())
        .build();
  }

  public static String saveTransaction(LakehouseStorage storage, RunningTransaction transaction) {
    String runningRootNodeFilePath = FileLocations.newNodeFilePath();
    TreeOperations.writeRootNodeFile(storage, runningRootNodeFilePath, transaction.runningRoot());
    TransactionDef transactionDef =
        TransactionDef.newBuilder()
            .setId(transaction.transactionId())
            .setIsolationLevel(transaction.isolationLevel())
            .setBeginningRootNodeFilePath(transaction.beginningRoot().path().get())
            .setRunningRootNodeFilePath(runningRootNodeFilePath)
            .setBeganAtMillis(transaction.beganAtMillis())
            .setExpireAtMillis(transaction.expireAtMillis())
            .build();
    String transactionDefFilePath = FileLocations.newTransactionDefFilePath(transaction);
    ObjectDefinitions.writeTransactionDef(storage, transactionDefFilePath, transactionDef);
    return transactionDefFilePath;
  }

  public static RunningTransaction loadTransaction(
      LakehouseStorage storage, String transactionDefFilePath) {
    TransactionDef transactionDef =
        ObjectDefinitions.readTransactionDef(storage, transactionDefFilePath);
    TreeRoot beginningRoot =
        TreeOperations.readRootNodeFile(storage, transactionDef.getBeginningRootNodeFilePath());
    TreeRoot runningRoot =
        TreeOperations.readRootNodeFile(storage, transactionDef.getRunningRootNodeFilePath());
    return ImmutableRunningTransaction.builder()
        .transactionId(transactionDef.getId())
        .beginningRoot(beginningRoot)
        .runningRoot(runningRoot)
        .beganAtMillis(transactionDef.getBeganAtMillis())
        .expireAtMillis(transactionDef.getExpireAtMillis())
        .isolationLevel(transactionDef.getIsolationLevel())
        .build();
  }

  public static List<String> showNamespaces(
      LakehouseStorage storage, RunningTransaction transaction) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    return transaction.runningRoot().nodeKeyTable().stream()
        .map(NodeKeyTableRow::key)
        .filter(key -> ObjectKeys.isNamespaceKey(key, lakehouseDef))
        .map(key -> ObjectKeys.namespaceNameFromKey(key, lakehouseDef))
        .collect(Collectors.toList());
  }

  public static boolean namespaceExists(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    return TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent();
  }

  public static NamespaceDef describeNamespace(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    Optional<String> namespaceDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey);
    if (!namespaceDefFilePath.isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    return ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath.get());
  }

  public static RunningTransaction createNamespace(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectAlreadyExistsException("Namespace %s already exists", namespaceName);
    }

    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, namespaceKey, namespaceDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction alterNamespace(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, namespaceKey, namespaceDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction dropNamespace(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.removeKey(storage, newRoot, namespaceKey);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static List<String> showTables(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    return transaction.runningRoot().nodeKeyTable().stream()
        .map(NodeKeyTableRow::key)
        .filter(key -> ObjectKeys.isTableKey(key, lakehouseDef))
        .map(key -> ObjectKeys.tableNameFromKey(key, lakehouseDef))
        .collect(Collectors.toList());
  }

  public static boolean tableExists(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, lakehouseDef);
    return TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent();
  }

  public static TableDef describeTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, lakehouseDef);
    Optional<String> tableDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey);
    if (!tableDefFilePath.isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exist", namespaceName, tableName);
    }
    return ObjectDefinitions.readTableDef(storage, tableDefFilePath.get());
  }

  public static RunningTransaction createTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, lakehouseDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectAlreadyExistsException(
          "Namespace %s table %s already exists", namespaceName, tableName);
    }

    String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, tableKey, tableDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction alterTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, tableKey, tableDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction dropTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.removeKey(storage, newRoot, tableKey);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static List<String> showViews(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    return transaction.runningRoot().nodeKeyTable().stream()
        .map(NodeKeyTableRow::key)
        .filter(key -> ObjectKeys.isViewKey(key, lakehouseDef))
        .map(key -> ObjectKeys.viewNameFromKey(key, lakehouseDef))
        .collect(Collectors.toList());
  }

  public static boolean viewExists(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String viewName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, lakehouseDef);
    return TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent();
  }

  public static ViewDef describeView(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String viewName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, lakehouseDef);
    Optional<String> viewDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey);
    if (!viewDefFilePath.isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exist", namespaceName, viewName);
    }
    return ObjectDefinitions.readViewDef(storage, viewDefFilePath.get());
  }

  public static RunningTransaction createView(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String viewName,
      ViewDef viewDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, lakehouseDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectAlreadyExistsException(
          "Namespace %s view %s already exists", namespaceName, viewName);
    }

    String viewDefFilePath = FileLocations.newViewDefFilePath(namespaceName, viewName);
    ObjectDefinitions.writeViewDef(storage, viewDefFilePath, namespaceName, viewName, viewDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, viewKey, viewDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction replaceView(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String viewName,
      ViewDef viewDef) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exists", namespaceName, viewName);
    }

    String viewDefFilePath = FileLocations.newViewDefFilePath(namespaceName, viewName);
    ObjectDefinitions.writeViewDef(storage, viewDefFilePath, namespaceName, viewName, viewDef);
    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.setValue(storage, newRoot, viewKey, viewDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction dropView(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String viewName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, lakehouseDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exists", namespaceName, viewName);
    }

    TreeRoot newRoot = TreeOperations.cloneTreeRoot(transaction.runningRoot());
    TreeOperations.removeKey(storage, newRoot, viewKey);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }
}
