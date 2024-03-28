/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.utils

import org.apache.gluten.vectorized.JniLibLoader

class SharedLibraryLoaderCentos8 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader
      .newTransaction()
      .loadAndCreateLink("libboost_atomic.so.1.84.0", "libboost_atomic.so", false)
      .loadAndCreateLink("libboost_thread.so.1.84.0", "libboost_thread.so", false)
      .loadAndCreateLink("libboost_system.so.1.84.0", "libboost_system.so", false)
      .loadAndCreateLink("libicudata.so.60", "libicudata.so", false)
      .loadAndCreateLink("libicuuc.so.60", "libicuuc.so", false)
      .loadAndCreateLink("libicui18n.so.60", "libicui18n.so", false)
      .loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so", false)
      .loadAndCreateLink("libboost_program_options.so.1.84.0", "libboost_program_options.so", false)
      .loadAndCreateLink("libboost_filesystem.so.1.84.0", "libboost_filesystem.so", false)
      .loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libevent-2.1.so.6", "libevent-2.1.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libglog.so.0", "libglog.so", false)
      .loadAndCreateLink("libdwarf.so.1", "libdwarf.so", false)
      .loadAndCreateLink("libidn.so.11", "libidn.so", false)
      .loadAndCreateLink("libntlm.so.0", "libntlm.so", false)
      .loadAndCreateLink("libgsasl.so.7", "libgsasl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .loadAndCreateLink("libre2.so.0", "libre2.so", false)
      .loadAndCreateLink("libsodium.so.23", "libsodium.so", false)
      .commit()
  }
}
