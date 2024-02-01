#!/bin/bash
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

set -exu
# New build option may need to be included in get_build_summary to ensure EP build cache workable.
# Enable S3 connector.
ENABLE_S3=OFF
# Enable GCS connector.
ENABLE_GCS=OFF
# Enable HDFS connector.
ENABLE_HDFS=OFF
# Enable ABFS connector.
ENABLE_ABFS=OFF
BUILD_TYPE=release
VELOX_HOME=""
ENABLE_EP_CACHE=OFF
# May be deprecated in Gluten build.
ENABLE_BENCHMARK=OFF
# May be deprecated in Gluten build.
ENABLE_TESTS=OFF
# Set to ON for gluten cpp test build.
BUILD_TEST_UTILS=OFF
RUN_SETUP_SCRIPT=ON
COMPILE_ARROW_JAVA=OFF
NUM_THREADS=""
OTHER_ARGUMENTS=""

OS=`uname -s`
ARCH=`uname -m`

for arg in "$@"; do
  case $arg in
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_gcs=*)
    ENABLE_GCS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_abfs=*)
    ENABLE_ABFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_type=*)
    BUILD_TYPE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_ep_cache=*)
    ENABLE_EP_CACHE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_test_utils=*)
    BUILD_TEST_UTILS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_tests=*)
    ENABLE_TESTS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    ENABLE_BENCHMARK=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --run_setup_script=*)
    RUN_SETUP_SCRIPT=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --compile_arrow_java=*)
    COMPILE_ARROW_JAVA=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --num_threads=*)
    NUM_THREADS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function compile {
  if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ $RUN_SETUP_SCRIPT == "ON" ]; then
    if [ $OS == 'Linux' ]; then
      setup_linux
    elif [ $OS == 'Darwin' ]; then
      setup_macos
    else
      echo "Unsupported kernel: $OS"
      exit 1
    fi
  fi

  COMPILE_OPTION="-DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=OFF"
  if [ $BUILD_TEST_UTILS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TEST_UTILS=ON"
  fi
  if [ $ENABLE_HDFS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
  fi
  # If ENABLE_BENCHMARK == ON, Velox disables tests and connectors
  if [ $ENABLE_BENCHMARK == "OFF" ]; then
    if [ $ENABLE_TESTS == "ON" ]; then
        COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TESTING=ON "
    fi
    if [ $ENABLE_ABFS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_ABFS=ON"
    fi
    if [ $ENABLE_GCS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_GCS=ON"
    fi
  else
    echo "ENABLE_BENCHMARK is ON. Disabling Tests, GCS and ABFS connectors if enabled."
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_BENCHMARKS=ON"
  fi

  COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
  COMPILE_TYPE=$(if [[ "$BUILD_TYPE" == "debug" ]] || [[ "$BUILD_TYPE" == "Debug" ]]; then echo 'debug'; else echo 'release'; fi)
  echo "COMPILE_OPTION: "$COMPILE_OPTION

  NUM_THREADS_OPTS=""
  if [ -n "${NUM_THREADS:-}" ]; then
    NUM_THREADS_OPTS="NUM_THREADS=$NUM_THREADS MAX_HIGH_MEM_JOBS=$NUM_THREADS MAX_LINK_JOBS=$NUM_THREADS"
  fi
  echo "NUM_THREADS_OPTS: $NUM_THREADS_OPTS"

  export simdjson_SOURCE=BUNDLED
  if [ $ARCH == 'x86_64' ]; then
    make $COMPILE_TYPE $NUM_THREADS_OPTS EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  elif [[ "$ARCH" == 'arm64' || "$ARCH" == 'aarch64' ]]; then
    CPU_TARGET=$ARCH make $COMPILE_TYPE $NUM_THREADS_OPTS EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  else
    echo "Unsupported arch: $ARCH"
    exit 1
  fi

  # Install deps to system as needed
  if [ -d "_build/$COMPILE_TYPE/_deps" ]; then
    cd _build/$COMPILE_TYPE/_deps
    if [ -d xsimd-build ]; then
      echo "INSTALL xsimd."
      if [ $OS == 'Linux' ]; then
        sudo cmake --install xsimd-build/
      elif [ $OS == 'Darwin' ]; then
        sudo cmake --install xsimd-build/
      fi
    fi
    if [ -d gtest-build ]; then
      echo "INSTALL gtest."
      if [ $OS == 'Linux' ]; then
        sudo cmake --install gtest-build/
      elif [ $OS == 'Darwin' ]; then
        sudo cmake --install gtest-build/
      fi
    fi
  fi
}

function get_build_summary {
  COMMIT_HASH=$1
  # Ideally all script arguments should be put into build summary.
  # ENABLE_EP_CACHE is excluded. Thus, in current build with ENABLE_EP_CACHE=ON, we can use EP cache
  # from last build with ENABLE_EP_CACHE=OFF,
  echo "ENABLE_S3=$ENABLE_S3,ENABLE_GCS=$ENABLE_GCS,ENABLE_HDFS=$ENABLE_HDFS,ENABLE_ABFS=$ENABLE_ABFS,\
BUILD_TYPE=$BUILD_TYPE,VELOX_HOME=$VELOX_HOME,ENABLE_BENCHMARK=$ENABLE_BENCHMARK,\
ENABLE_TESTS=$ENABLE_TESTS,BUILD_TEST_UTILS=$BUILD_TEST_UTILS,\
COMPILE_ARROW_JAVA=$COMPILE_ARROW_JAVA,OTHER_ARGUMENTS=$OTHER_ARGUMENTS,COMMIT_HASH=$COMMIT_HASH"
}

function check_commit {
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -f ${VELOX_HOME}/velox-build.cache ]; then
      CACHED_BUILD_SUMMARY="$(cat ${VELOX_HOME}/velox-build.cache)"
      if [ -n "$CACHED_BUILD_SUMMARY" ]; then
        if [ "$TARGET_BUILD_SUMMARY" = "$CACHED_BUILD_SUMMARY" ]; then
          echo "Velox build $TARGET_BUILD_SUMMARY was cached."
          exit 0
        else
          echo "Found cached build $CACHED_BUILD_SUMMARY for Velox which is different with target build $TARGET_BUILD_SUMMARY."
        fi
      fi
    fi
  else
    # Branch-new build requires all untracked files to be deleted. We only need the source code.
    git clean -dffx :/
  fi

  if [ -f ${VELOX_HOME}/velox-build.cache ]; then
    rm -f ${VELOX_HOME}/velox-build.cache
  fi
}

function setup_macos {
  if [ $ARCH == 'x86_64' ]; then
    ./scripts/setup-macos.sh
  elif [ $ARCH == 'arm64' ]; then
    CPU_TARGET="arm64" ./scripts/setup-macos.sh
  else
    echo "Unknown arch: $ARCH"
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    scripts/setup-ubuntu.sh
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
    8) scripts/setup-centos8.sh ;;
    7)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    *)
      echo "Unsupported centos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
    2)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    3) scripts/setup-centos8.sh ;;
    *)
      echo "Unsupported alinux version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
    3.2) scripts/setup-centos8.sh ;;
    *)
      echo "Unsupported tencentos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  else
    echo "Unsupported linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}

function compile_arrow_java_module() {
    ARROW_HOME="${VELOX_HOME}/_build/$COMPILE_TYPE/third_party/arrow_ep/src/arrow_ep"
    ARROW_INSTALL_DIR="${ARROW_HOME}/../../install"

    pushd $ARROW_HOME/java
    mvn clean install -pl maven/module-info-compiler-maven-plugin -am \
          -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip

    # Arrow C Data Interface CPP libraries
    mvn generate-resources -P generate-libs-cdata-all-os -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow Java libraries
    mvn clean install -P arrow-c-data -pl c -am -DskipTests -Dcheckstyle.skip \
      -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip
    popd
}

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_GCS=${ENABLE_GCS}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "ENABLE_ABFS=${ENABLE_ABFS}"
echo "BUILD_TYPE=${BUILD_TYPE}"

cd ${VELOX_HOME}
TARGET_BUILD_SUMMARY=$(get_build_summary "$(git rev-parse --verify HEAD)")
if [ -z "$TARGET_BUILD_SUMMARY" ]; then
  echo "Unable to parse Velox build: $TARGET_BUILD_SUMMARY."
  exit 1
fi
echo "Target Velox build: $TARGET_BUILD_SUMMARY"

check_commit
compile

if [ $COMPILE_ARROW_JAVA == "ON" ]; then
  compile_arrow_java_module
fi

echo "Successfully built Velox from Source."
echo $TARGET_BUILD_SUMMARY >"${VELOX_HOME}/velox-build.cache"
