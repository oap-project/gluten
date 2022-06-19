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

cmake_minimum_required(VERSION 3.16)

project(velox)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")

set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/x86_64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")

if(NOT DEFINED VELOX_HOME)
  message(FATAL_ERROR "VELOX_HOME must be set to the source directory of Velox")
else()
  message(STATUS "VELOX_HOME: ${VELOX_HOME}")
endif()

if(NOT DEFINED VELOX_BUILD_TYPE)
  set(VELOX_BUILD_TYPE release
      CACHE STRING "The build type specified when the Velox binary was built.")
endif()
string(TOLOWER "${VELOX_BUILD_TYPE}" LOWERCASE_VELOX_BUILD_TYPE)

# User can specify VELOX_BUILD_PATH, if Velox are built elsewhere.
if(NOT DEFINED VELOX_BUILD_PATH)
  set(VELOX_BUILD_PATH "${VELOX_HOME}/_build/${LOWERCASE_VELOX_BUILD_TYPE}"
      CACHE PATH "Velox build directory.")
endif()

set(VELOX_COMPONENTS_PATH "${VELOX_BUILD_PATH}/velox")

function(ADD_VELOX_DEPENDENCY VELOX_DEP_LIB_NAME VELOX_DEP_LIB_PATH)
  if(NOT EXISTS ${VELOX_DEP_LIB_PATH})
    message(FATAL_ERROR "Velox library not exists: ${VELOX_DEP_LIB_PATH}")
  endif()
  set(VELOX_DEP_LIB facebook::velox::${VELOX_DEP_LIB_NAME})
  add_library(${VELOX_DEP_LIB} STATIC IMPORTED)
  set_target_properties(${VELOX_DEP_LIB} PROPERTIES
      IMPORTED_LOCATION ${VELOX_DEP_LIB_PATH})
  target_link_libraries(velox PUBLIC ${VELOX_DEP_LIB})
endfunction()

macro(ADD_VELOX_DEPENDENCIES)
  add_velox_dependency(functions::sparksql::lib "${VELOX_COMPONENTS_PATH}/functions/sparksql/libvelox_functions_spark.a")
  add_velox_dependency(functions::prestosql::agg "${VELOX_COMPONENTS_PATH}/functions/prestosql/aggregates/libvelox_aggregates.a")
  add_velox_dependency(exec "${VELOX_COMPONENTS_PATH}/exec/libvelox_exec.a")
  add_velox_dependency(substrait "${VELOX_COMPONENTS_PATH}/substrait/libvelox_substrait_plan_converter.a")
  add_velox_dependency(functions::prestosql "${VELOX_COMPONENTS_PATH}/functions/prestosql/registration/libvelox_functions_prestosql.a")
  add_velox_dependency(functions::prestosql::impl "${VELOX_COMPONENTS_PATH}/functions/prestosql/libvelox_functions_prestosql_impl.a")
  add_velox_dependency(functions::json "${VELOX_COMPONENTS_PATH}/functions/prestosql/json/libvelox_functions_json.a")
  add_velox_dependency(functions::hyperloglog "${VELOX_COMPONENTS_PATH}/functions/prestosql/hyperloglog/libvelox_functions_hyperloglog.a")
  add_velox_dependency(functions::lib "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_lib.a")
  add_velox_dependency(exec::test::util "${VELOX_COMPONENTS_PATH}/exec/tests/utils/libvelox_exec_test_util.a")
  add_velox_dependency(parse::parser "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_parser.a")
  add_velox_dependency(parse::expression "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_expression.a")
  add_velox_dependency(vector::arrow::bridge "${VELOX_COMPONENTS_PATH}/vector/arrow/libvelox_arrow_bridge.a")

  add_velox_dependency(connector::hive "${VELOX_COMPONENTS_PATH}/connectors/hive/libvelox_hive_connector.a")
  add_velox_dependency(filesystems::hdfs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/hdfs/libvelox_hdfs.a")
  add_velox_dependency(connector "${VELOX_COMPONENTS_PATH}/connectors/libvelox_connector.a")

  add_velox_dependency(dwio::dwrf::test_utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/test/utils/libvelox_dwrf_test_utils.a")
  add_velox_dependency(dwio::dwrf::writer "${VELOX_COMPONENTS_PATH}/dwio/dwrf/writer/libvelox_dwio_dwrf_writer.a")
  add_velox_dependency(dwio::dwrf::reader "${VELOX_COMPONENTS_PATH}/dwio/dwrf/reader/libvelox_dwio_dwrf_reader.a")
  add_velox_dependency(dwio::dwrf::utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/utils/libvelox_dwio_dwrf_utils.a")
  add_velox_dependency(dwio::dwrf::common "${VELOX_COMPONENTS_PATH}/dwio/dwrf/common/libvelox_dwio_dwrf_common.a")
  add_velox_dependency(parquet "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_reader.a")
  add_velox_dependency(parquet::reader::duckdb "${VELOX_COMPONENTS_PATH}/dwio/parquet/duckdb_reader/libvelox_dwio_duckdb_parquet_reader.a")
  add_velox_dependency(parquet::duckdb::reader "${VELOX_COMPONENTS_PATH}/dwio/parquet/duckdb_reader/duckdb/libvelox_dwio_duckdb_parquet_reader_duckdb.a")
  add_velox_dependency(dwio::common "${VELOX_COMPONENTS_PATH}/dwio/common/libvelox_dwio_common.a")

  add_velox_dependency(duckdb::function "${VELOX_COMPONENTS_PATH}/duckdb/functions/libvelox_duckdb_functions.a")
  add_velox_dependency(duckdb::conversion "${VELOX_COMPONENTS_PATH}/duckdb/conversion/libvelox_duckdb_conversion.a")

  add_velox_dependency(expression "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression.a")
  add_velox_dependency(core "${VELOX_COMPONENTS_PATH}/core/libvelox_core.a")

  add_velox_dependency(type "${VELOX_COMPONENTS_PATH}/type/libvelox_type.a")
  add_velox_dependency(vector "${VELOX_COMPONENTS_PATH}/vector/libvelox_vector.a")
  add_velox_dependency(expression::function "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression_functions.a")
  add_velox_dependency(expression::type_calculation "${VELOX_COMPONENTS_PATH}/expression/type_calculation/libvelox_type_calculation.a")

  add_velox_dependency(common::caching "${VELOX_COMPONENTS_PATH}/common/caching/libvelox_caching.a")
  add_velox_dependency(common::base "${VELOX_COMPONENTS_PATH}/common/base/libvelox_common_base.a")
  add_velox_dependency(common::memory "${VELOX_COMPONENTS_PATH}/common/memory/libvelox_memory.a")
  add_velox_dependency(common::serialization "${VELOX_COMPONENTS_PATH}/common/serialization/libvelox_serialization.a")
  add_velox_dependency(common::base::exception "${VELOX_COMPONENTS_PATH}/common/base/libvelox_exception.a")

  add_velox_dependency(type::tz "${VELOX_COMPONENTS_PATH}/type/tz/libvelox_type_tz.a")
  add_velox_dependency(functions::prestosql::types "${VELOX_COMPONENTS_PATH}/functions/prestosql/types/libvelox_presto_types.a")
  add_velox_dependency(dwio::dwrf::proto "${VELOX_COMPONENTS_PATH}/dwio/dwrf/proto/libvelox_dwio_dwrf_proto.a")
  add_velox_dependency(dwio::common::exception "${VELOX_COMPONENTS_PATH}/dwio/common/exception/libvelox_dwio_common_exception.a")
  add_velox_dependency(dwio::common::encryption "${VELOX_COMPONENTS_PATH}/dwio/common/encryption/libvelox_dwio_common_encryption.a")
  add_velox_dependency(dwio::common::compression "${VELOX_COMPONENTS_PATH}/dwio/common/compression/libvelox_dwio_common_compression.a")

  add_velox_dependency(core::config "${VELOX_COMPONENTS_PATH}/core/libvelox_config.a")
  add_velox_dependency(common::encode "${VELOX_COMPONENTS_PATH}/common/encode/libvelox_encode.a")
  add_velox_dependency(common::time "${VELOX_COMPONENTS_PATH}/common/time/libvelox_time.a")
  add_velox_dependency(common::file "${VELOX_COMPONENTS_PATH}/common/file/libvelox_file.a")
  add_velox_dependency(common::process "${VELOX_COMPONENTS_PATH}/common/process/libvelox_process.a")

  add_velox_dependency(external::md5 "${VELOX_COMPONENTS_PATH}/external/md5/libmd5.a")
  add_velox_dependency(external::date "${VELOX_COMPONENTS_PATH}/external/date/libvelox_external_date.a")
  add_velox_dependency(external::duckdb "${VELOX_COMPONENTS_PATH}/external/duckdb/libduckdb.a")
  add_velox_dependency(external::duckdb::tpch "${VELOX_COMPONENTS_PATH}/external/duckdb/tpch/libtpch_extension.a")
  add_velox_dependency(external::duckdb::tpch::dbgen "${VELOX_COMPONENTS_PATH}/external/duckdb/tpch/dbgen/libdbgen.a")

endmacro()

macro(find_libhdfs3)
  # Find the libhdfs3 library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(LIBHDFS3_LIBRARY NAMES hdfs3 PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT LIBHDFS3_LIBRARY)
     message(FATAL_ERROR "LIBHDFS3 Library Not Found")
  else()
    message(STATUS "LIBHDFS3 Library Can Be Found in ${LIBHDFS3_LIBRARY}")
  endif()
endmacro()

# Build Velox backend.
set(VELOX_SRCS
    jni/jni_wrapper.cc
    compute/VeloxPlanConverter.cc
    compute/ArrowTypeUtils.cc
    compute/VeloxToRowConverter.cc
    compute/DwrfDatasource.cc
    compute/bridge.cc
    )
add_library(velox SHARED ${VELOX_SRCS})

target_include_directories(velox PUBLIC
    ${CMAKE_SYSTEM_INCLUDE_PATH}
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${root_directory}/src
    ${VELOX_HOME}
    ${VELOX_BUILD_PATH}
    ${VELOX_HOME}/velox/vector
    ${VELOX_HOME}/third_party/xsimd/include/)

set_target_properties(velox PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

find_package(Folly REQUIRED CONFIG)
find_package(gflags REQUIRED COMPONENTS shared CONFIG)

target_link_libraries(velox PUBLIC spark_columnar_jni)
add_velox_dependencies()
target_link_libraries(velox PUBLIC Folly::folly)

find_libhdfs3()
add_library(libhdfs3 SHARED IMPORTED)
set_target_properties(libhdfs3
                      PROPERTIES IMPORTED_LOCATION "${LIBHDFS3_LIBRARY}")
target_link_libraries(velox PUBLIC libhdfs3)

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(BUILD_TESTS)
  add_test_case(shuffle_test SOURCES tests/shuffle_split_test.cc)
endif()
