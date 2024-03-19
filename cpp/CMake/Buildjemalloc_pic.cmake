# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Building Jemalloc
macro(build_jemalloc)
  message(STATUS "Building Jemalloc from Source")

  if(DEFINED ENV{GLUTEN_JEMALLOC_URL})
    set(JEMALLOC_SOURCE_URL "$ENV{GLUTEN_JEMALLOC_URL}")
  else()
    set(JEMALLOC_BUILD_VERSION "5.2.1")
    set(JEMALLOC_SOURCE_URL
            "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_BUILD_VERSION}/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
            "https://github.com/ursa-labs/thirdparty/releases/download/latest/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
            )
  endif()

  set(JEMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-install")
  set(JEMALLOC_LIB_DIR "${JEMALLOC_PREFIX}/lib")
  set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
  set(
    JEMALLOC_STATIC_LIB
    "${JEMALLOC_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    JEMALLOC_INCLUDE
    "${JEMALLOC_PREFIX}/include"
    )
  set(JEMALLOC_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "CC=${CMAKE_C_COMPILER}"
      "--prefix=${JEMALLOC_PREFIX}"
      "--libdir=${JEMALLOC_LIB_DIR}"
      "--with-jemalloc-prefix=je_gluten_"
      "--with-private-namespace=je_gluten_private_"
      "--without-export"
      "--disable-shared"
      "--disable-cxx"
      "--disable-libdl"
      # For fixing an issue when loading native lib: cannot allocate memory in static TLS block.
      "--disable-initial-exec-tls"
      "CFLAGS=-fPIC"
      "CXXFLAGS=-fPIC")
  set(JEMALLOC_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  ExternalProject_Add(jemalloc_ep
                      URL ${JEMALLOC_SOURCE_URL}
                      PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
                      CONFIGURE_COMMAND "./configure" ${JEMALLOC_CONFIGURE_ARGS}
                      BUILD_COMMAND ${JEMALLOC_BUILD_COMMAND}
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
                      INSTALL_COMMAND make install)

  file(MAKE_DIRECTORY "${JEMALLOC_INCLUDE_DIR}")
  add_library(jemalloc::libjemalloc STATIC IMPORTED)
  set_target_properties(
    jemalloc::libjemalloc
    PROPERTIES INTERFACE_LINK_LIBRARIES Threads::Threads
               IMPORTED_LOCATION "${JEMALLOC_STATIC_LIB}"
               INTERFACE_INCLUDE_DIRECTORIES
               "${JEMALLOC_INCLUDE_DIR}")
  add_dependencies(jemalloc::libjemalloc jemalloc_ep)
endmacro()
