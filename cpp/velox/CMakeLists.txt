# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)

project(velox)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)
include(FindPackageHandleStandardArgs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
endif()

set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/x86_64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL aarch64)
  set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/aarch64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL arm64)
  set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib" CACHE PATH "System Lib MultiArch dir")
else()
  message(FATAL_ERROR "Unsupported processor type: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

if (NOT DEFINED VELOX_HOME)
  set(VELOX_HOME ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
  message(STATUS "Set VELOX_HOME to ${VELOX_HOME}")
endif()

# User can specify VELOX_BUILD_PATH, if Velox are built elsewhere.
if(NOT DEFINED VELOX_BUILD_PATH)
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(VELOX_BUILD_PATH "${VELOX_HOME}/_build/debug" CACHE PATH "Velox build directory.")
  else()
    set(VELOX_BUILD_PATH "${VELOX_HOME}/_build/release" CACHE PATH "Velox build directory.")
  endif()
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

macro(ADD_VELOX_OBJECTS)
  add_library(velox_objects OBJECT IMPORTED GLOBAL)
  set_property(TARGET velox_objects PROPERTY IMPORTED_OBJECTS
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/FileHandle.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HiveConfig.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HiveConnector.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HiveDataSink.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HiveDataSource.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HivePartitionUtil.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/PartitionIdGenerator.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/SplitReader.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/TableHandle.cpp.o"
          "${VELOX_COMPONENTS_PATH}/connectors/hive/CMakeFiles/velox_hive_connector.dir/HiveConnectorUtil.cpp.o"
          )
  target_link_libraries(velox PUBLIC velox_objects)
endmacro()

macro(add_duckdb)
  find_package(DuckDB)
  if(NOT DuckDB_FOUND)
    message(STATUS "Use duckdb provided by Velox.")
    set(VELOX_BUILD_DUCKDB_PATH "${VELOX_BUILD_PATH}/_deps/duckdb-build")
    add_velox_dependency(duckdb::static "${VELOX_BUILD_DUCKDB_PATH}/src/libduckdb_static.a")
    if(${BUILD_JEMALLOC_EXTENSION})
      add_velox_dependency(duckdb::jemalloc "${VELOX_BUILD_DUCKDB_PATH}/extension/jemalloc/libjemalloc_extension.a")
    endif()
    add_velox_dependency(duckdb::fmt "${VELOX_BUILD_DUCKDB_PATH}/third_party/fmt/libduckdb_fmt.a")
    add_velox_dependency(duckdb::query "${VELOX_BUILD_DUCKDB_PATH}/third_party/libpg_query/libduckdb_pg_query.a")
    add_velox_dependency(duckdb::re2 "${VELOX_BUILD_DUCKDB_PATH}/third_party/re2/libduckdb_re2.a")
    add_velox_dependency(duckdb::miniz "${VELOX_BUILD_DUCKDB_PATH}/third_party/miniz/libduckdb_miniz.a")
    add_velox_dependency(duckdb::utf8 "${VELOX_BUILD_DUCKDB_PATH}/third_party/utf8proc/libduckdb_utf8proc.a")
    add_velox_dependency(duckdb::hyperloglog "${VELOX_BUILD_DUCKDB_PATH}/third_party/hyperloglog/libduckdb_hyperloglog.a")
    add_velox_dependency(duckdb::fastpforlib "${VELOX_BUILD_DUCKDB_PATH}/third_party/fastpforlib/libduckdb_fastpforlib.a")
    add_velox_dependency(duckdb::mbedtls "${VELOX_BUILD_DUCKDB_PATH}/third_party/mbedtls/libduckdb_mbedtls.a")
    add_velox_dependency(duckdb::fsst "${VELOX_BUILD_DUCKDB_PATH}/third_party/fsst/libduckdb_fsst.a")
  else()
    message(STATUS "Found DuckDB library from ${DuckDB_DIR}")
    target_link_libraries(velox PUBLIC duckdb_static)
  endif()
endmacro()

macro(ADD_VELOX_DEPENDENCIES)
  add_velox_objects()
  add_velox_dependency(expression::sigparser "${VELOX_COMPONENTS_PATH}/expression/signature_parser/libvelox_signature_parser.a")
  add_velox_dependency(functions::sparksql::lib "${VELOX_COMPONENTS_PATH}/functions/sparksql/libvelox_functions_spark.a")
  add_velox_dependency(functions::sparksql::agg "${VELOX_COMPONENTS_PATH}/functions/sparksql/aggregates/libvelox_functions_spark_aggregates.a")
  add_velox_dependency(functions::window::sparksql "${VELOX_COMPONENTS_PATH}/functions/sparksql/window/libvelox_functions_spark_window.a")
  add_velox_dependency(functions::prestosql::agg "${VELOX_COMPONENTS_PATH}/functions/prestosql/aggregates/libvelox_aggregates.a")
  add_velox_dependency(functions::lib::agg "${VELOX_COMPONENTS_PATH}/functions/lib/aggregates/libvelox_functions_aggregates.a")
  add_velox_dependency(functions::prestosql::window "${VELOX_COMPONENTS_PATH}/functions/prestosql/window/libvelox_window.a")
  add_velox_dependency(functions::lib::window "${VELOX_COMPONENTS_PATH}/functions/lib/window/libvelox_functions_window.a")
  add_velox_dependency(velox::buffer "${VELOX_COMPONENTS_PATH}/buffer/libvelox_buffer.a")

  add_velox_dependency(functions::isnull "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_is_null_functions.a")
  add_velox_dependency(functions::prestosql "${VELOX_COMPONENTS_PATH}/functions/prestosql/registration/libvelox_functions_prestosql.a")
  add_velox_dependency(functions::prestosql::impl "${VELOX_COMPONENTS_PATH}/functions/prestosql/libvelox_functions_prestosql_impl.a")
  add_velox_dependency(functions::json "${VELOX_COMPONENTS_PATH}/functions/prestosql/json/libvelox_functions_json.a")
  add_velox_dependency(functions::hyperloglog "${VELOX_COMPONENTS_PATH}/common/hyperloglog/libvelox_common_hyperloglog.a")
  add_velox_dependency(functions::lib "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_lib.a")
  if(BUILD_TESTS)
    add_velox_dependency(exec::test "${VELOX_COMPONENTS_PATH}/exec/tests/utils/libvelox_exec_test_lib.a")
    add_velox_dependency(temp::path "${VELOX_COMPONENTS_PATH}/exec/tests/utils/libvelox_temp_path.a")
    add_velox_dependency(dwio::common::test::utils "${VELOX_COMPONENTS_PATH}/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a")
  endif()
  add_velox_dependency(exec "${VELOX_COMPONENTS_PATH}/exec/libvelox_exec.a")
  
  if(BUILD_TESTS)
    add_velox_dependency(parse::parser "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_parser.a")
    add_velox_dependency(duckdb::parser "${VELOX_COMPONENTS_PATH}/duckdb/conversion/libvelox_duckdb_parser.a")
    add_velox_dependency(parse::expression "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_expression.a")
    add_velox_dependency(parse::utils "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_utils.a")
    add_velox_dependency(function::registry "${VELOX_COMPONENTS_PATH}/functions/libvelox_function_registry.a")
  endif()
  add_velox_dependency(vector::arrow::bridge "${VELOX_COMPONENTS_PATH}/vector/arrow/libvelox_arrow_bridge.a")
  add_velox_dependency(row "${VELOX_COMPONENTS_PATH}/row/libvelox_row_fast.a")
  add_velox_dependency(connector "${VELOX_COMPONENTS_PATH}/connectors/libvelox_connector.a")
  add_velox_dependency(connector::hive_parition "${VELOX_COMPONENTS_PATH}/connectors/hive/libvelox_hive_partition_function.a")
  add_velox_dependency(connector::hive::iceberg::IcebergSplitReader "${VELOX_COMPONENTS_PATH}/connectors/hive/iceberg/libvelox_hive_iceberg_splitreader.a")
  if(ENABLE_HDFS)
    add_velox_dependency(connector::hive::hdfs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/hdfs/libvelox_hdfs.a")
  endif()
  if(ENABLE_GCS)
    add_velox_dependency(connector::hive::gcs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/gcs/libvelox_gcs.a")
  endif()
  if(ENABLE_S3)
    add_velox_dependency(connector::hive::s3fs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/s3fs/libvelox_s3fs.a")
  endif()
  if(ENABLE_ABFS)
  add_velox_dependency(connector::hive::abfs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/abfs/libvelox_abfs.a")
  endif()
  add_velox_dependency(dwio::dwrf::writer "${VELOX_COMPONENTS_PATH}/dwio/dwrf/writer/libvelox_dwio_dwrf_writer.a")
  add_velox_dependency(dwio::dwrf::reader "${VELOX_COMPONENTS_PATH}/dwio/dwrf/reader/libvelox_dwio_dwrf_reader.a")
  add_velox_dependency(dwio::dwrf::utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/utils/libvelox_dwio_dwrf_utils.a")
  add_velox_dependency(dwio::dwrf::common "${VELOX_COMPONENTS_PATH}/dwio/dwrf/common/libvelox_dwio_dwrf_common.a")
  add_velox_dependency(parquet "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_reader.a")
  add_velox_dependency(parquet::reader::native "${VELOX_COMPONENTS_PATH}/dwio/parquet/reader/libvelox_dwio_native_parquet_reader.a")
  if(BUILD_TESTS)
    add_velox_dependency(dwio::common::utils "${VELOX_COMPONENTS_PATH}/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a")
    add_velox_dependency(dwio::dwrf::test_utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/test/utils/libvelox_dwrf_test_utils.a")
    add_velox_dependency(parquet::reader::duckdb_conversion "${VELOX_COMPONENTS_PATH}/duckdb/conversion/libvelox_duckdb_conversion.a")

    add_duckdb()
    
    add_velox_dependency(tpch::gen "${VELOX_COMPONENTS_PATH}/tpch/gen/libvelox_tpch_gen.a")
    add_velox_dependency(dbgen "${VELOX_COMPONENTS_PATH}/tpch/gen/dbgen/libvelox_dbgen.a")
  endif()

  add_velox_dependency(parquet::reader::thrift "${VELOX_COMPONENTS_PATH}/dwio/parquet/thrift/libvelox_dwio_parquet_thrift.a")

  add_velox_dependency(velox::arrow::parquet::writer "${VELOX_COMPONENTS_PATH}/dwio/parquet/writer/libvelox_dwio_arrow_parquet_writer.a")
  add_velox_dependency(dwio::arrow::parquet::writer "${VELOX_COMPONENTS_PATH}/dwio/parquet/writer/arrow/libvelox_dwio_arrow_parquet_writer_lib.a")
  add_velox_dependency(dwio::arrow::parquet::writer::util "${VELOX_COMPONENTS_PATH}/dwio/parquet/writer/arrow/util/libvelox_dwio_arrow_parquet_writer_util_lib.a")
  add_velox_dependency(dwio::arrow::parquet::writer::thrift::lib "${VELOX_COMPONENTS_PATH}/dwio/parquet/writer/arrow/generated/libvelox_dwio_arrow_parquet_writer_thrift_lib.a")
  add_velox_dependency(dwio::common::compression "${VELOX_COMPONENTS_PATH}/dwio/common/compression/libvelox_dwio_common_compression.a")
  add_velox_dependency(dwio::common "${VELOX_COMPONENTS_PATH}/dwio/common/libvelox_dwio_common.a")
  add_velox_dependency(functions::prestosql::types "${VELOX_COMPONENTS_PATH}/functions/prestosql/types/libvelox_presto_types.a")
  add_velox_dependency(functions::spark::specialforms "${VELOX_COMPONENTS_PATH}/functions/sparksql/specialforms/libvelox_functions_spark_specialforms.a")
  add_velox_dependency(expression "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression.a")
  add_velox_dependency(core "${VELOX_COMPONENTS_PATH}/core/libvelox_core.a")

  add_velox_dependency(type::fbhive "${VELOX_COMPONENTS_PATH}/type/fbhive/libvelox_type_fbhive.a")
  add_velox_dependency(type "${VELOX_COMPONENTS_PATH}/type/libvelox_type.a")
  add_velox_dependency(vector::serializes "${VELOX_COMPONENTS_PATH}/serializers/libvelox_presto_serializer.a")
  add_velox_dependency(functions::lib::util "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_util.a")
  add_velox_dependency(vector "${VELOX_COMPONENTS_PATH}/vector/libvelox_vector.a")
  add_velox_dependency(expression::function "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression_functions.a")
  add_velox_dependency(expression::type_calculation "${VELOX_COMPONENTS_PATH}/expression/type_calculation/libvelox_type_calculation.a")

  add_velox_dependency(common::caching "${VELOX_COMPONENTS_PATH}/common/caching/libvelox_caching.a")
  add_velox_dependency(common::base "${VELOX_COMPONENTS_PATH}/common/base/libvelox_common_base.a")
  add_velox_dependency(common::memory "${VELOX_COMPONENTS_PATH}/common/memory/libvelox_memory.a")
  add_velox_dependency(common::serialization "${VELOX_COMPONENTS_PATH}/common/serialization/libvelox_serialization.a")
  add_velox_dependency(common::base::exception "${VELOX_COMPONENTS_PATH}/common/base/libvelox_exception.a")

  add_velox_dependency(type::tz "${VELOX_COMPONENTS_PATH}/type/tz/libvelox_type_tz.a")
  add_velox_dependency(dwio::dwrf::proto "${VELOX_COMPONENTS_PATH}/dwio/dwrf/proto/libvelox_dwio_dwrf_proto.a")
  add_velox_dependency(dwio::catalog::fbhive "${VELOX_COMPONENTS_PATH}/dwio/catalog/fbhive/libvelox_dwio_catalog_fbhive.a")
  add_velox_dependency(dwio::common::exception "${VELOX_COMPONENTS_PATH}/dwio/common/exception/libvelox_dwio_common_exception.a")
  add_velox_dependency(dwio::common::encryption "${VELOX_COMPONENTS_PATH}/dwio/common/encryption/libvelox_dwio_common_encryption.a")

  add_velox_dependency(core::config "${VELOX_COMPONENTS_PATH}/core/libvelox_config.a")
  add_velox_dependency(common::encode "${VELOX_COMPONENTS_PATH}/common/encode/libvelox_encode.a")
  add_velox_dependency(common::time "${VELOX_COMPONENTS_PATH}/common/time/libvelox_time.a")
  add_velox_dependency(common::file "${VELOX_COMPONENTS_PATH}/common/file/libvelox_file.a")
  add_velox_dependency(common::process "${VELOX_COMPONENTS_PATH}/common/process/libvelox_process.a")

  add_velox_dependency(common::test_util "${VELOX_COMPONENTS_PATH}/common/testutil/libvelox_test_util.a")

  add_velox_dependency(external::md5 "${VELOX_COMPONENTS_PATH}/external/md5/libmd5.a")
  add_velox_dependency(external::date "${VELOX_COMPONENTS_PATH}/external/date/libvelox_external_date.a")
  add_velox_dependency(velox::parquet::writer "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_writer.a")

  if(BUILD_TESTS)
    add_velox_dependency(vector::test::util "${VELOX_COMPONENTS_PATH}/vector/tests/utils/libvelox_vector_test_lib.a")
  endif()
  add_velox_dependency(common::compression "${VELOX_COMPONENTS_PATH}/common/compression/libvelox_common_compression.a")
  add_velox_dependency(common::io "${VELOX_COMPONENTS_PATH}/common/io/libvelox_common_io.a")
  add_velox_dependency(velox::status "${VELOX_COMPONENTS_PATH}/common/base/libvelox_status.a")
  add_velox_dependency(external::simdjson "${VELOX_BUILD_PATH}/_deps/simdjson-build/libsimdjson.a")
endmacro()

macro(find_libhdfs3)
  find_package(libhdfs3 CONFIG)
  if(libhdfs3_FOUND AND TARGET HDFS::hdfs3)
    set(LIBHDFS3_LIBRARY HDFS::hdfs3)
  else()
    find_path(libhdfs3_INCLUDE_DIR hdfs/hdfs.h)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    find_library(libhdfs3_LIBRARY NAMES hdfs3)
    find_package_handle_standard_args(libhdfs3 DEFAULT_MSG
      libhdfs3_INCLUDE_DIR
      libhdfs3_LIBRARY
    )
    add_library(HDFS::hdfs3 SHARED IMPORTED)
      set_target_properties(HDFS::hdfs3 PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${libhdfs3_INCLUDE_DIR}"
      IMPORTED_LOCATION "${libhdfs3_LIBRARY}"
    )
  endif()

  if (NOT libhdfs3_FOUND)
    message(FATAL_ERROR "LIBHDFS3 Library Not Found")
  endif()
endmacro()

macro(find_re2)
  find_package(re2 CONFIG)
  if(re2_FOUND AND TARGET re2::re2)
    set(RE2_LIBRARY re2::re2)
  else()
    find_library(RE2_LIBRARY NAMES re2 PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  endif()

  if (NOT RE2_LIBRARY)
     message(FATAL_ERROR "RE2 Library Not Found")
  else()
    message(STATUS "RE2 Library Can Be Found in ${RE2_LIBRARY}")
  endif()
endmacro()

macro(find_awssdk)
  set (CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
endmacro()

macro(find_gcssdk)
  set (CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_package(google_cloud_cpp_storage REQUIRED)
endmacro()

macro(find_azure)
  find_package(CURL REQUIRED)
  find_package(LibXml2 REQUIRED)
  set (CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
  find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
endmacro()

# Build Velox backend.
set(VELOX_SRCS
    compute/VeloxBackend.cc
    compute/VeloxRuntime.cc
    compute/WholeStageResultIterator.cc
    compute/VeloxPlanConverter.cc
    compute/iceberg/IcebergPlanConverter.cc
    jni/VeloxJniWrapper.cc
    jni/JniFileSystem.cc
    jni/JniUdf.cc
    memory/VeloxColumnarBatch.cc
    memory/VeloxMemoryManager.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/functions/RowConstructorWithNull.cc
    operators/functions/SparkTokenizer.cc
    operators/serializer/VeloxColumnarToRowConverter.cc
    operators/serializer/VeloxColumnarBatchSerializer.cc
    operators/serializer/VeloxRowToColumnarConverter.cc
    operators/writer/VeloxParquetDatasource.cc
    shuffle/VeloxShuffleReader.cc
    shuffle/VeloxShuffleWriter.cc
    substrait/SubstraitParser.cc
    substrait/SubstraitToVeloxExpr.cc
    substrait/SubstraitToVeloxPlan.cc
    substrait/SubstraitToVeloxPlanValidator.cc
    substrait/VariantToVectorConverter.cc
    substrait/SubstraitExtensionCollector.cc
    substrait/VeloxSubstraitSignature.cc
    substrait/VeloxToSubstraitExpr.cc
    substrait/VeloxToSubstraitPlan.cc
    substrait/VeloxToSubstraitType.cc
    udf/UdfLoader.cc
    utils/VeloxArrowUtils.cc
    utils/ConfigExtractor.cc
    utils/Common.cc
    )

if (ENABLE_HDFS)
  list(APPEND VELOX_SRCS utils/HdfsUtils.cc)
endif ()

if(ENABLE_S3)
  find_package(ZLIB)
endif()

if(BUILD_TESTS OR BUILD_BENCHMARKS)
  list(APPEND VELOX_SRCS utils/tests/MemoryPoolUtils.cc)
endif()

add_library(velox SHARED ${VELOX_SRCS})

if(ENABLE_GLUTEN_VCPKG)
  # Hide symbols of static dependencies
  target_link_options(velox PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()

target_include_directories(velox PUBLIC
    ${CMAKE_SYSTEM_INCLUDE_PATH}
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${VELOX_HOME}/
    ${VELOX_BUILD_PATH}/
    ${VELOX_BUILD_PATH}/_deps/xsimd-src/include/
    ${VELOX_HOME}/third_party/xsimd/include/)

set_target_properties(velox PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

## If folly is not installed in system lib paths, please add
## `-DCMAKE_PREFIX_PATH="${folly lib path}" to cmake arguments.
## It is also applicable to other dependencies.
find_package(Folly REQUIRED CONFIG)

target_include_directories(velox PUBLIC
        ${GTEST_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE})

target_link_libraries(velox PUBLIC gluten)
add_velox_dependencies()

# Arrow libraries appear after Velox dependencies to avoid linker error
find_arrow_lib(${ARROW_LIB_NAME})
find_arrow_lib(${PARQUET_LIB_NAME})
target_link_libraries(velox PUBLIC Arrow::arrow Arrow::parquet)

target_link_libraries(velox PUBLIC Folly::folly)
find_re2()
target_link_libraries(velox PUBLIC ${RE2_LIBRARY})

if(ENABLE_GLUTEN_VCPKG)
  find_package(Thrift CONFIG)
else()
  # Prefer the shared library on system.
  set(ARROW_THRIFT_USE_SHARED ON)
  find_package(Thrift)
endif()

if(Thrift_FOUND)
  target_link_libraries(velox PUBLIC thrift::thrift)
else()
  add_velox_dependency(thrift "${ARROW_HOME}/src/arrow_ep-build/thrift_ep-install/lib/libthrift.a")
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(ENABLE_HDFS)
  add_definitions(-DENABLE_HDFS)
  find_libhdfs3()
  target_link_libraries(velox PUBLIC HDFS::hdfs3)
endif()

if(ENABLE_S3)
  add_definitions(-DENABLE_S3)
  find_awssdk()
  target_link_libraries(velox PUBLIC ${AWSSDK_LIBRARIES})
endif()

if(ENABLE_GCS)
  add_definitions(-DENABLE_GCS)
  find_gcssdk()
  target_link_libraries(velox PUBLIC google-cloud-cpp::storage)
endif()

if(BUILD_EXAMPLES)
  add_subdirectory(udf/examples)
endif()

if(ENABLE_ABFS)
  add_definitions(-DENABLE_ABFS)
  find_azure()
  target_link_libraries(velox PUBLIC Azure::azure-storage-blobs)
  target_link_libraries(velox PUBLIC Azure::azure-storage-files-datalake)
endif()

add_custom_command(TARGET velox POST_BUILD COMMAND ld $<TARGET_FILE:velox> || true
        COMMENT "Checking ld result of libvelox.so")
add_custom_command(TARGET velox POST_BUILD COMMAND ldd $<TARGET_FILE:velox> || true
        COMMENT "Checking ldd result of libvelox.so")
