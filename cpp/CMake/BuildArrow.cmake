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

include(ExternalProject)

set(ARROW_EP_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-install")
message(STATUS "ARROW_EP_INSTALL_PREFIX: ${ARROW_EP_INSTALL_PREFIX}")

set(ARROW_EP_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
message(STATUS "ARROW_EP_SOURCE_DIR: ${ARROW_EP_SOURCE_DIR}")

set(ARROW_INCLUDE_DIR "${ARROW_EP_INSTALL_PREFIX}/include")
set(BINARY_RELEASE_DIR "${root_directory}/releases")

ExternalProject_Add(arrow_ep
    GIT_REPOSITORY https://github.com/oap-project/arrow.git
    SOURCE_DIR ${ARROW_EP_SOURCE_DIR}
    GIT_TAG gluten-main
    BUILD_IN_SOURCE 1
    INSTALL_DIR ${ARROW_EP_INSTALL_PREFIX}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
    -DARROW_BUILD_STATIC=OFF
    -DARROW_BUILD_SHARED=ON
    -DARROW_SUBSTRAIT=ON
    -DARROW_COMPUTE=ON
    -DARROW_S3=ON
    -DARROW_GANDIVA_JAVA=ON
    -DARROW_GANDIVA=ON
    -DARROW_PARQUET=ON
    -DARROW_CSV=ON
    -DARROW_HDFS=ON
    -DARROW_BOOST_USE_SHARED=OFF
    -DARROW_JNI=ON
    -DARROW_DATASET=ON
    -DARROW_WITH_PROTOBUF=ON
    -DARROW_PROTOBUF_USE_SHARED=OFF
    -DARROW_WITH_SNAPPY=ON
    -DARROW_WITH_LZ4=ON
    -DARROW_WITH_ZSTD=OFF
    -DARROW_WITH_BROTLI=OFF
    -DARROW_WITH_ZLIB=OFF
    -DARROW_FILESYSTEM=ON
    -DARROW_JSON=ON
    -DARROW_FLIGHT=OFF
    -DARROW_JEMALLOC=ON
    -DARROW_SIMD_LEVEL=AVX2
    -DARROW_RUNTIME_SIMD_LEVEL=MAX
    -DARROW_DEPENDENCY_SOURCE=BUNDLED
    -DCMAKE_INSTALL_PREFIX=${ARROW_EP_INSTALL_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib)

ExternalProject_Add_Step(arrow_ep java_c_abi
    COMMAND sh -c "mkdir -p build && cd build && cmake .. && cmake --build ."
    COMMENT "Build Arrow Java C Data Interface"
    DEPENDEES mkdir download update patch configure build install
    WORKING_DIRECTORY "${ARROW_EP_SOURCE_DIR}/java/c"
    )

ExternalProject_Add_Step(arrow_ep java_install
    COMMAND mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=${ARROW_EP_INSTALL_PREFIX}/lib -DskipTests -Dcheckstyle.skip
    COMMENT "Arrow Java maven install after CPP make install"
    DEPENDEES mkdir download update patch configure build install java_c_abi
    WORKING_DIRECTORY "${ARROW_EP_SOURCE_DIR}/java"
    )

# Copy Arrow Headers to releases/include
ExternalProject_Add_Step(arrow_ep copy_arrow_header
    COMMAND cp -rf ${ARROW_EP_INSTALL_PREFIX}/include/ ${root_directory}/releases/
    COMMENT "Arrow Header to releases/include"
    DEPENDEES mkdir download update patch configure build install java_install
    WORKING_DIRECTORY "${ARROW_EP_INSTALL_PREFIX}/"
    )

add_dependencies(arrow_ep jni_proto)

message(STATUS "Building Static ARROW: ${STATIC_ARROW}")

if(STATIC_ARROW)
  # Load Static Arrow Library
  message(FATAL_ERROR "Not Support Static Arrow")

  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)

  set(ARROW_LIB_NAME arrow_bundled_dependencies)

  set(
      ARROW_STATIC_LIB
      "${ARROW_EP_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  add_library(Arrow::arrow STATIC IMPORTED)
  set_target_properties(Arrow::arrow
      PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${ARROW_EP_INSTALL_PREFIX}/include")
  add_dependencies(Arrow::arrow arrow_ep)

  # Load Static Gandiva Library
  set(
      GANDIVA_STATIC_LIB
      "${ARROW_EP_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  add_library(Arrow::gandiva STATIC IMPORTED)
  set_target_properties(Arrow::gandiva
      PROPERTIES IMPORTED_LOCATION "${GANDIVA_STATIC_LIB}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${ARROW_EP_INSTALL_PREFIX}/include")
  add_dependencies(Arrow::gandiva arrow_ep)

endif()
