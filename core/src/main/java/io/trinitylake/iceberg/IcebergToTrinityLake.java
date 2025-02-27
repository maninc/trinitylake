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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergToTrinityLake {

  private IcebergToTrinityLake() {}

  public static IcebergNamespaceParseResult parseNamespace(
      Namespace namespace, TrinityLakeIcebergCatalogProperties catalogProperties) {
    ValidationUtil.checkArgument(
        !namespace.isEmpty(), "Empty namespace is not allowed in TrinityLake");

    if (!catalogProperties.systemNamespaceName().equals(namespace.level(0))) {
      ValidationUtil.checkArgument(
          namespace.length() == 1, "Namespace name must only have 1 level");
      return ImmutableIcebergNamespaceParseResult.builder()
          .isSystem(false)
          .namespaceName(namespace.level(0))
          .build();
    }

    if (namespace.length() == 1) {
      return ImmutableIcebergNamespaceParseResult.builder()
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
      return ImmutableIcebergNamespaceParseResult.builder()
          .isSystem(false)
          .distTransactionId(
              distTransactionId.substring(catalogProperties.dtxnNamespacePrefix().length()))
          .namespaceName(namespace.level(3))
          .build();
    }

    throw new InvalidArgumentException(
        "Unknown parent namespace name in system namespace: %s", parentNamespaceName);
  }

  public static String tableName(TableIdentifier tableIdentifier) {
    // TODO: add name validations
    return tableIdentifier.name();
  }

  public static String fullTableName(
      String catalogName,
      IcebergNamespaceParseResult parseResult,
      TableIdentifier tableIdentifier) {
    return String.format(
        "%s.%s.%s", catalogName, parseResult.namespaceName(), tableIdentifier.name());
  }
}
