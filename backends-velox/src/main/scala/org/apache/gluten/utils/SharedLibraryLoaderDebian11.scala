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

class SharedLibraryLoaderDebian11 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader
      .newTransaction()
      .loadAndCreateLink("libicudata.so.67", "libicudata.so", false)
      .loadAndCreateLink("libre2.so.9", "libre2.so", false)
      .loadAndCreateLink("libicuuc.so.67", "libicuuc.so", false)
      .loadAndCreateLink("liblber-2.4.so.2", "liblber-2.4.so", false)
      .loadAndCreateLink("libsasl2.so.2", "libsasl2.so", false)
      .loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so", false)
      .loadAndCreateLink("libicui18n.so.67", "libicui18n.so", false)
      .loadAndCreateLink("libunwind.so.8", "libunwind.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so", false)
      .loadAndCreateLink("librtmp.so.1", "librtmp.so", false)
      .loadAndCreateLink("libssh2.so.1", "libssh2.so", false)
      .loadAndCreateLink("libpsl.so.5", "libpsl.so", false)
      .loadAndCreateLink("libldap_r-2.4.so.2", "libldap_r-2.4.so", false)
      .loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so", false)
      .loadAndCreateLink("libthrift-0.13.0.so", "libthrift.so", false)
      .loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so", false)
      .loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libglog.so.0", "libglog.so", false)
      .loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so", false)
      .loadAndCreateLink("libsnappy.so.1", "libsnappy.so", false)
      .loadAndCreateLink("libcurl.so.4", "libcurl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .commit()
  }
}
