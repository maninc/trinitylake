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

import io.trinitylake.exception.InvalidArgumentException;
import io.trinitylake.util.ValidationUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergToTrinityLake {

  private IcebergToTrinityLake() {}

  public static IcebergNamespaceInfo parseNamespace(
      Namespace namespace, TrinityLakeIcebergCatalogProperties catalogProperties) {
    ValidationUtil.checkArgument(
        !namespace.isEmpty(), "Empty namespace is not allowed in TrinityLake");

    if (!catalogProperties.systemNamespaceName().equals(namespace.level(0))) {
      ValidationUtil.checkArgument(
          namespace.length() == 1, "Namespace name must only have 1 level");
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(false)
          .namespaceName(namespace.level(0))
          .build();
    }

    if (namespace.length() == 1) {
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(true)
          .namespaceName(catalogProperties.systemNamespaceName())
          .build();
    }

    String parentNamespaceName = namespace.level(1);
    if (catalogProperties.dtxnParentNamespaceName().equals(parentNamespaceName)) {
      String distTransactionId = namespace.level(2);
      ValidationUtil.checkArgument(
          distTransactionId.startsWith(catalogProperties.dtxnNamespacePrefix()),
          "Distributed transaction namespace name must start with %s",
          catalogProperties.dtxnNamespacePrefix());
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(false)
          .distTransactionId(
              distTransactionId.substring(catalogProperties.dtxnNamespacePrefix().length()))
          .namespaceName(namespace.level(3))
          .build();
    }

    throw new InvalidArgumentException(
        "Unknown parent namespace name in system namespace: %s", parentNamespaceName);
  }

  public static IcebergTableInfo parseTableIdentifier(
      TableIdentifier tableIdentifier, TrinityLakeIcebergCatalogProperties catalogProperties) {
    Namespace namespace = tableIdentifier.namespace();

    ValidationUtil.checkArgument(
        !namespace.isEmpty(), "Empty namespace is not allowed in TrinityLake");

    if (!catalogProperties.systemNamespaceName().equals(namespace.level(0))) {
      ValidationUtil.checkArgument(
          namespace.length() <= 2, "Namespace name must only have 1 level");

      if (namespace.length() == 1) {
        return ImmutableIcebergTableInfo.builder()
            .namespaceName(namespace.level(0))
            .tableName(tableIdentifier.name())
            .build();
      }

      MetadataTableType metadataTableType = MetadataTableType.from(tableIdentifier.name());
      ValidationUtil.checkArgument(
          metadataTableType != null,
          "Unknown metadata table type %s in table identifier: %s",
          tableIdentifier.name(),
          tableIdentifier);
      return ImmutableIcebergTableInfo.builder()
          .namespaceName(namespace.level(0))
          .tableName(namespace.level(1))
          .metadataTableType(metadataTableType)
          .build();
    }

    ValidationUtil.checkArgument(
        namespace.length() == 3 || namespace.length() == 4,
        "TrinityLake system namespace does not directly contain any table");

    String subSystemNamespace = namespace.level(1);
    ValidationUtil.checkArgument(
        catalogProperties.dtxnParentNamespaceName().equals(subSystemNamespace),
        "TrinityLake system namespace does not contain any table in sub-namespace: %s",
        subSystemNamespace);

    String distTransactionNamespace = namespace.level(2);
    ValidationUtil.checkArgument(
        distTransactionNamespace.startsWith(catalogProperties.dtxnNamespacePrefix()),
        "Distributed transaction namespace name must start with %s",
        catalogProperties.dtxnNamespacePrefix());

    String distTransactionId =
        distTransactionNamespace.substring(catalogProperties.dtxnNamespacePrefix().length());

    if (namespace.length() == 3) {
      return ImmutableIcebergTableInfo.builder()
          .distTransactionId(distTransactionId)
          .namespaceName(namespace.level(2))
          .tableName(tableIdentifier.name())
          .build();
    }

    MetadataTableType metadataTableType = MetadataTableType.from(tableIdentifier.name());
    ValidationUtil.checkArgument(
        metadataTableType != null,
        "Unknown metadata table type %s in table identifier: %s",
        tableIdentifier.name(),
        tableIdentifier);
    return ImmutableIcebergTableInfo.builder()
        .distTransactionId(distTransactionId)
        .namespaceName(namespace.level(2))
        .tableName(namespace.level(3))
        .metadataTableType(metadataTableType)
        .build();
  }
}
