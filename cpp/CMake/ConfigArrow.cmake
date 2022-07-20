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

set(ARROW_SHARED_LIBRARY_SUFFIX ".so.900")
set(ARROW_SHARED_LIBRARY_PARENT_SUFFIX ".so.900.0.0")

set(ARROW_LIB_NAME "arrow")
set(GANDIVA_LIB_NAME "gandiva")
set(PARQUET_LIB_NAME "parquet")
set(ARROW_DATASET_LIB_NAME "arrow_dataset")
set(ARROW_SUBSTRAIT_LIB_NAME "arrow_substrait")

function(FIND_ARROW_LIB LIB_NAME)
  if(NOT TARGET Arrow::${LIB_NAME})
    set(ARROW_LIB_FULL_NAME ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX})
    add_library(Arrow::${LIB_NAME} SHARED IMPORTED)
    set_target_properties(Arrow::${LIB_NAME}
        PROPERTIES IMPORTED_LOCATION "${root_directory}/releases/${ARROW_LIB_FULL_NAME}"
        INTERFACE_INCLUDE_DIRECTORIES
        "${root_directory}/releases/include")

    if(BUILD_ARROW)
      set(ARROW_LIB_PARENT_FULL_NAME ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${ARROW_SHARED_LIBRARY_PARENT_SUFFIX})
      add_custom_command(
          OUTPUT ${root_directory}/releases/${ARROW_LIB_PARENT_FULL_NAME} ${root_directory}/releases/${ARROW_LIB_FULL_NAME}
          COMMAND cp -a ${ARROW_EP_INSTALL_PREFIX}/lib/${ARROW_LIB_PARENT_FULL_NAME} ${root_directory}/releases/
          COMMAND cp -a ${ARROW_EP_INSTALL_PREFIX}/lib/${ARROW_LIB_FULL_NAME} ${root_directory}/releases/
          DEPENDS arrow_ep)
      add_custom_target(COPY_LIB_${LIB_NAME} ALL
          DEPENDS ${root_directory}/releases/${ARROW_LIB_FULL_NAME})
      add_dependencies(Arrow::${LIB_NAME} COPY_LIB_${LIB_NAME})
    else()
      find_library(ARROW_LIB_${LIB_NAME}
          NAMES ${ARROW_LIB_FULL_NAME}
          PATHS ${ARROW_BFS_LIB_DIR} ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR}
          NO_DEFAULT_PATH)
      if(NOT ARROW_LIB_${LIB_NAME})
        message(FATAL_ERROR "Arrow Library Not Found: ${ARROW_LIB_FULL_NAME}")
      else()
        message(STATUS "Found Arrow Library: ${ARROW_LIB_${LIB_NAME}}")
      endif()
      file(COPY ${ARROW_LIB_${LIB_NAME}} DESTINATION ${root_directory}/releases/ FOLLOW_SYMLINK_CHAIN)
    endif()
  endif()
endfunction()

if(BUILD_ARROW)
  include(BuildArrow)
else()
  message(STATUS "Use existing ARROW libraries")

  set(ARROW_BFS_INSTALL_DIR "/usr/local" CACHE PATH "Arrow Build from Source dir")
  set(ARROW_BFS_LIB_DIR "${ARROW_BFS_INSTALL_DIR}/lib")
  set(ARROW_BFS_INCLUDE_DIR "${ARROW_BFS_INSTALL_DIR}/include")

  set(ARROW_ROOT "/usr/local" CACHE PATH "Arrow Root dir")
  set(ARROW_LIB_DIR "${ARROW_ROOT}/lib")
  set(ARROW_LIB64_DIR "${ARROW_ROOT}/lib64")
  set(ARROW_INCLUDE_DIR "${ARROW_ROOT}/include")

  message(STATUS "Set Arrow Library Directory in ${ARROW_BFS_LIB_DIR} or ${ARROW_LIB_DIR} or ${ARROW_LIB64_DIR}")
  message(STATUS "Set Arrow Include Directory in ${ARROW_BFS_INCLUDE_DIR} or ${ARROW_INCLUDE_DIR}")

  if(EXISTS ${ARROW_BFS_INCLUDE_DIR}/arrow)
    set(ARROW_INCLUDE_SRC_DIR ${ARROW_BFS_INCLUDE_DIR})
  elseif(EXISTS ${ARROW_INCLUDE_DIR}/arrow)
    set(ARROW_INCLUDE_SRC_DIR ${ARROW_INCLUDE_DIR})
  else()
    message(FATAL_ERROR "Arrow headers not found either in ARROW_BFS_INSTALL_DIR or ARROW_ROOT.")
  endif()

  # Copy arrow headers
  set(ARROW_INCLUDE_DST_DIR ${root_directory}/releases/include)
  set(ARROW_INCLUDE_SUB_DIR arrow gandiva parquet)
  message(STATUS "Copy Arrow headers from ${ARROW_INCLUDE_SRC_DIR} to ${ARROW_INCLUDE_DST_DIR}")
  file(MAKE_DIRECTORY ${ARROW_INCLUDE_DST_DIR})
  foreach(SUB_DIR ${ARROW_INCLUDE_SUB_DIR})
    file(COPY ${ARROW_INCLUDE_SRC_DIR}/${SUB_DIR} DESTINATION ${ARROW_INCLUDE_DST_DIR})
  endforeach()
endif()
