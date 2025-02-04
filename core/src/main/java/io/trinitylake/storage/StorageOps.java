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
package io.trinitylake.storage;

import io.trinitylake.Initializable;
import io.trinitylake.storage.local.LocalInputStream;
import java.io.Closeable;
import java.util.List;

/** Common operations that should be supported by a TrinityLake storage */
public interface StorageOps extends Closeable, Initializable {

  StorageOpsProperties commonProperties();

  StorageOpsProperties systemSpecificProperties();

  void prepareToRead(URI uri);

  SeekableInputStream startRead(URI uri);

  LocalInputStream startReadLocal(URI uri);

  AtomicOutputStream startWrite(URI uri);

  boolean exists(URI uri);

  void delete(List<URI> uris);

  List<URI> list(URI prefix);
}
