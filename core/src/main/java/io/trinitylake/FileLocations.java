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

import io.trinitylake.relocated.com.google.common.hash.HashCode;
import io.trinitylake.relocated.com.google.common.hash.HashFunction;
import io.trinitylake.relocated.com.google.common.hash.Hashing;
import io.trinitylake.util.ValidationUtil;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

public class FileLocations {

  public static final String LATEST_VERSION_HINT_FILE_PATH = "_latest_hint.txt";

  public static final String LAKEHOUSE_DEF_FILE_PATH_PREFIX = "_lakehouse_def_";

  public static final String PROTOBUF_BINARY_FILE_SUFFIX = ".binpb";

  public static final String ARROW_FILE_SUFFIX = ".arrow";

  private static final String ARROW_FILE_SUFFIX_REVERSED = "worra.";

  private static final int ARROW_FILE_SUFFIX_LENGTH = ARROW_FILE_SUFFIX.length();

  private static final int ROOT_NODE_FILE_VERSION_BINARY_LENGTH = 64;

  private static final int ROOT_NODE_FILE_PATH_LENGTH =
      ROOT_NODE_FILE_VERSION_BINARY_LENGTH + ARROW_FILE_SUFFIX_LENGTH + 1;

  private static final Pattern ROOT_NODE_FILE_PATH_PATTERN = Pattern.compile("^_[01]{64}\\.arrow$");

  private static final String VIEW_DEF_FILE_PREFIX = "view";

  private static final String TRANSACTION_DEF_FILE_PREFIX = "txn";

  private static final String NODE_FILE_PREFIX = "node";

  private static final HashFunction HASH_FUNC = Hashing.murmur3_32_fixed();

  // Length of entropy generated in the file path
  private static final int HASH_BINARY_STRING_BITS = 20;

  // Entropy generated will be divided into dirs with this lengths
  private static final int ENTROPY_DIR_LENGTH = 4;

  // Entropy generated will be divided into this number of directories
  private static final int ENTROPY_DIR_DEPTH = 3;

  private FileLocations() {}

  public static boolean isRootNodeFilePath(String path) {
    return ROOT_NODE_FILE_PATH_PATTERN.matcher(path).matches();
  }

  public static long versionFromNodeFilePath(String path) {
    ValidationUtil.checkArgument(
        isRootNodeFilePath(path),
        "Root node file path must match pattern: %s",
        ROOT_NODE_FILE_PATH_PATTERN);
    String reversedBinary = path.substring(1, path.length() - ARROW_FILE_SUFFIX_LENGTH);
    String binary = new StringBuilder().append(reversedBinary).reverse().toString();
    return Long.parseLong(binary, 2);
  }

  public static String rootNodeFilePath(long version) {
    ValidationUtil.checkArgument(version >= 0, "version must be non-negative");
    StringBuilder sb = new StringBuilder(ROOT_NODE_FILE_PATH_LENGTH);
    String binaryLong = Long.toBinaryString(version);
    sb.append(ARROW_FILE_SUFFIX_REVERSED);
    for (int i = 0; i < ROOT_NODE_FILE_VERSION_BINARY_LENGTH - binaryLong.length(); i++) {
      sb.append("0");
    }
    sb.append(binaryLong);
    sb.append("_");
    return sb.reverse().toString();
  }

  public static String newLakehouseDefFilePath() {
    return LAKEHOUSE_DEF_FILE_PATH_PREFIX + UUID.randomUUID() + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String newNamespaceDefFilePath(String namespaceName) {
    return generateOptimizedFilePath(
        PROTOBUF_BINARY_FILE_SUFFIX, namespaceName, UUID.randomUUID().toString());
  }

  public static String newTableDefFilePath(String namespaceName, String tableName) {
    return generateOptimizedFilePath(
        PROTOBUF_BINARY_FILE_SUFFIX, namespaceName, tableName, UUID.randomUUID().toString());
  }

  public static String newViewDefFilePath(String namespaceName, String viewName) {
    return generateOptimizedFilePath(
        PROTOBUF_BINARY_FILE_SUFFIX,
        VIEW_DEF_FILE_PREFIX,
        namespaceName,
        viewName,
        UUID.randomUUID().toString());
  }

  public static String distTransactionDefFilePath(String transactionId) {
    return generateOptimizedFilePath(
        PROTOBUF_BINARY_FILE_SUFFIX, TRANSACTION_DEF_FILE_PREFIX, transactionId);
  }

  public static String newNodeFilePath() {
    return generateOptimizedFilePath(
        ARROW_FILE_SUFFIX, NODE_FILE_PREFIX, UUID.randomUUID().toString());
  }

  private static String generateOptimizedFilePath(String suffix, String... parts) {
    String originalName = String.join("-", parts) + suffix;
    return computeHash(originalName) + "-" + originalName;
  }

  private static String computeHash(String fileName) {
    HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);

    // {@link Integer#toBinaryString} excludes leading zeros, which we want to preserve.
    // force the first bit to be set to get around that.
    String hashAsBinaryString = Integer.toBinaryString(hashCode.asInt() | Integer.MIN_VALUE);
    // Limit hash length to HASH_BINARY_STRING_BITS
    String hash =
        hashAsBinaryString.substring(hashAsBinaryString.length() - HASH_BINARY_STRING_BITS);
    return dirsFromHash(hash);
  }

  /**
   * Divides hash into directories for optimized listing access using ENTROPY_DIR_DEPTH and
   * ENTROPY_DIR_LENGTH
   *
   * @param hash 10011001100110011001
   * @return 1001/1001/1001/10011001 with depth 3 and length 4
   */
  private static String dirsFromHash(String hash) {
    StringBuilder hashWithDirs = new StringBuilder();

    for (int i = 0; i < ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH; i += ENTROPY_DIR_LENGTH) {
      if (i > 0) {
        hashWithDirs.append("/");
      }
      hashWithDirs.append(hash, i, Math.min(i + ENTROPY_DIR_LENGTH, hash.length()));
    }

    if (hash.length() > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH) {
      hashWithDirs.append("/").append(hash, ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH, hash.length());
    }

    return hashWithDirs.toString();
  }
}
