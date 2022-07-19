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

project(spark_columnar_jni)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(BUILD_PROTOBUF "Build Protobuf from Source" ON)
option(USE_AVX512 "Build with AVX-512 optimizations" OFF)
option(TESTS "Build the tests" OFF)
option(BENCHMARKS "Build the benchmarks" OFF)
option(BUILD_JEMALLOC "Build Jemalloc from Source" OFF)

set(BOOST_MIN_VERSION "1.42.0")
find_package(Boost REQUIRED)
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})

set(JEMALLOC_BUILD_VERSION "5.2.1")

set(source_root_directory ${CMAKE_CURRENT_SOURCE_DIR})
set(proto_directory ${CMAKE_CURRENT_SOURCE_DIR}/../../jvm/src/main/resources/substrait/proto)
set(substrait_proto_directory ${proto_directory}/substrait)
message(STATUS "Set Proto Directory in ${proto_directory}")

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# Building Protobuf
macro(build_protobuf)
  message(STATUS "Building Protocol Buffers from Source")
  set (PROTOBUF_SOURCE_URL
       "https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz"
        "https://github.com/ursa-labs/thirdparty/releases/download/latest/protobuf-v3.13.0.tar.gz"
  )
  set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set(
    PROTOBUF_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_BIN
    "${PROTOBUF_PREFIX}/bin/protoc"
    )
  set(
    PROTOBUF_INCLUDE
    "${PROTOBUF_PREFIX}/include"
    )
  set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")
  set(PROTOBUF_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "RANLIB=${CMAKE_RANLIB}"
      "CC=${CMAKE_C_COMPILER}"
      "CXX=${CMAKE_CXX_COMPILER}"
      "--disable-shared"
      "--prefix=${PROTOBUF_PREFIX}"
      "CFLAGS=-fPIC"
      "CXXFLAGS=-fPIC")
  set(PROTOBUF_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  ExternalProject_Add(protobuf_ep
                      PREFIX protobuf_ep
                      CONFIGURE_COMMAND "./configure" ${PROTOBUF_CONFIGURE_ARGS}
                      BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOBUF_COMPILER}"
                      BUILD_COMMAND ${PROTOBUF_BUILD_COMMAND}
                      BUILD_IN_SOURCE 1
                      URL_MD5 cafa623d51361228c83c874d95f51992
                      URL ${PROTOBUF_SOURCE_URL}
  )

  file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")
  add_library(protobuf::libprotobuf STATIC IMPORTED)
  set_target_properties(
    protobuf::libprotobuf
    PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${PROTOBUF_INCLUDE_DIR}")
  add_dependencies(protobuf::libprotobuf protobuf_ep)
endmacro()

macro(find_protobuf)
  # Find the existing Protobuf
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(Protobuf)
  if ("${Protobuf_LIBRARY}" STREQUAL "Protobuf_LIBRARY-NOTFOUND")
    message(FATAL_ERROR "Protobuf Library Not Found")
  endif()
  set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
endmacro()

if(USE_AVX512)
  # Only enable additional instruction sets if they are supported
  message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")
  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    set(AVX512_FLAG "-march=skylake-avx512")
    check_cxx_compiler_flag(${AVX512_FLAG} CXX_SUPPORTS_AVX512)
    if(NOT CXX_SUPPORTS_AVX512)
      message(FATAL_ERROR "AVX512 required but compiler doesn't support it.")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${AVX512_FLAG}")
    add_definitions(-DCOLUMNAR_PLUGIN_USE_AVX512)
  endif ()
endif()

# Building Jemalloc
macro(build_jemalloc)
  message(STATUS "Building Jemalloc from Source")
  set (JEMALLOC_SOURCE_URL
       "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_BUILD_VERSION}/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
       "https://github.com/ursa-labs/thirdparty/releases/download/latest/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
  )
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
      "--with-jemalloc-prefix=je_gazelle_"
      "--with-private-namespace=je_gazelle_private_"
      "--without-export"
      "--disable-shared"
      "--disable-cxx"
      "--disable-libdl"
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
  add_dependencies(jemalloc::libjemalloc protobuf_ep)
endmacro()

# Find Jemalloc
macro(find_jemalloc)
  # Find the existing Protobuf
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(jemalloc_pic)
  if ("${Jemalloc_LIBRARY}" STREQUAL "Jemalloc_LIBRARY-NOTFOUND")
    message(FATAL_ERROR "Jemalloc Library Not Found")
  endif()
  set(PROTOC_BIN ${Jemalloc_PROTOC_EXECUTABLE})
endmacro()

# Set up Proto
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(GLOB PROTO_FILES ${proto_directory}/*.proto
                      ${substrait_proto_directory}/*.proto
                      ${substrait_proto_directory}/extensions/*.proto)
FOREACH(PROTO ${PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${proto_directory} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  LIST(APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  LIST(APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
ENDFOREACH()
set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(PROTO_DIR ${proto_directory}/,
                       DIRECTORY)

set(CODEGEN_HEADERS
    third_party/
    )
set(THIRD_PARTY_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${THIRD_PARTY_INCLUDE})
file(MAKE_DIRECTORY ${root_directory}/releases/include)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/common/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/third_party/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/precompile/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/utils/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/arrow_compute/ext/)
file(COPY utils/metrics.h DESTINATION ${root_directory}/releases/include/common/)

add_definitions(-DNATIVESQL_SRC_PATH="${root_directory}/releases")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")
set(SPARK_COLUMNAR_PLUGIN_SRCS
        jni/jni_wrapper.cc
        jni/exec_backend.cc
        memory/allocator.cc
        memory/arrow_memory_pool.cc
        ${PROTO_SRCS}
        compute/protobuf_utils.cc
        operators/c2r/arrow_columnar_to_row_converter.cc
        operators/c2r/columnar_to_row_base.cc
        operators/shuffle/splitter.cc
        )

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(spark_columnar_jni SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
add_dependencies(spark_columnar_jni jni_proto)

find_arrow_lib(${ARROW_LIB_NAME})
find_arrow_lib(${GANDIVA_LIB_NAME})

if(BUILD_PROTOBUF)
  build_protobuf()
  message(STATUS "Building ProtoBuf from Source: ${BUILD_PROTOBUF}")
  target_link_libraries(spark_columnar_jni
                        LINK_PRIVATE protobuf::libprotobuf)
else()
  find_protobuf()
  message(STATUS "Use existing ProtoBuf libraries: ${PROTOBUF_LIBRARY}")
  target_link_libraries(spark_columnar_jni
                        LINK_PUBLIC ${PROTOBUF_LIBRARY})
endif()

add_custom_command(OUTPUT ${PROTO_OUTPUT_FILES}
                   COMMAND ${PROTOC_BIN}
                           --proto_path
                           ${proto_directory}/
                           --cpp_out
                           ${PROTO_OUTPUT_DIR}
                           ${PROTO_FILES}
                   DEPENDS  ${PROTO_DIR}
                   COMMENT "Running PROTO compiler"
                   VERBATIM)
add_custom_target(jni_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})
add_dependencies(jni_proto protobuf::libprotobuf)
target_include_directories(spark_columnar_jni PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${source_root_directory} ${PROTO_OUTPUT_DIR} ${PROTOBUF_INCLUDE})
set_target_properties(spark_columnar_jni PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

# Build Jemalloc
if(BUILD_JEMALLOC)
  build_jemalloc(${STATIC_JEMALLOC})
  message(STATUS "Building Jemalloc: ${STATIC_JEMALLOC}")
else() #
  find_jemalloc()
  message(STATUS "Use existing Jemalloc libraries")
endif()

if(BUILD_TESTS)
  add_test_case(exec_backend_test SOURCES jni/exec_backend_test.cc)
endif()

if(DEFINED ENV{HADOOP_HOME})
  set(LIBHDFS3_DESTINATION $ENV{HADOOP_HOME}/lib/native)
else()
  set(LIBHDFS3_DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

target_link_libraries(spark_columnar_jni
    PUBLIC Arrow::arrow Arrow::gandiva)

install(TARGETS spark_columnar_jni
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${source_root_directory}/resources/libhdfs.so
        DESTINATION ${LIBHDFS3_DESTINATION})
install(FILES ${source_root_directory}/resources/libprotobuf.so.13
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
