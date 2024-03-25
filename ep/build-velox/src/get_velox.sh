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

VELOX_REPO=https://github.com/gaoyangxiaozhu/velox.git
VELOX_BRANCH=gayangya/input_file_name_test
VELOX_HOME=""

#Set on run gluten on HDFS
ENABLE_HDFS=OFF
#It can be set to OFF when compiling velox again
BUILD_PROTOBUF=ON
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on GCS
ENABLE_GCS=OFF
#Set on run gluten on ABFS
ENABLE_ABFS=OFF

OS=`uname -s`

for arg in "$@"; do
  case $arg in
  --velox_repo=*)
    VELOX_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_branch=*)
    VELOX_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_protobuf=*)
    BUILD_PROTOBUF=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
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
  --enable_abfs=*)
    ENABLE_ABFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function process_setup_ubuntu {
  if [ -z "$(which git)" ]; then
    sudo --preserve-env apt install -y git
  fi
  # make this function Reentrant
  git checkout scripts/setup-ubuntu.sh

  # No need to re-install git.
  sed -i '/git \\/d' scripts/setup-ubuntu.sh
  # need set BUILD_SHARED_LIBS flag for thrift
  sed -i  "/facebook\/fbthrift/{n;s/cmake_install -DBUILD_TESTS=OFF/cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF/;}" scripts/setup-ubuntu.sh
  # Do not install libunwind which can cause interruption when catching native exception.
  sed -i 's/sudo --preserve-env apt install -y libunwind-dev && //' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  *thrift* \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  libxml2-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  libkrb5-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache /a\  libgsasl7-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  libuuid1 \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  uuid-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\  curl \\' scripts/setup-ubuntu.sh
  sed -i '/libre2-dev/d' scripts/setup-ubuntu.sh
  sed -i '/libgmock-dev/d' scripts/setup-ubuntu.sh # resolved by ep/build-velox/build/velox_ep/CMake/resolve_dependency_modules/gtest.cmake
  sed -i 's/github_checkout boostorg\/boost \"\${BOOST_VERSION}\" --recursive/wget_and_untar https:\/\/github.com\/boostorg\/boost\/releases\/download\/boost-1.84.0\/boost-1.84.0.tar.gz boost \&\& cd boost/g' scripts/setup-ubuntu.sh
  if [ $ENABLE_HDFS == "ON" ]; then
    sed -i '/^function install_folly.*/i function install_libhdfs3 {\n  github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
    sed -i '/ccache /a\  yasm \\' scripts/setup-ubuntu.sh
  fi
  if [ $BUILD_PROTOBUF == "ON" ]; then
    sed -i '/^function install_folly.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
  fi
  sed -i "s/apt install -y/sudo apt install -y/" ${VELOX_HOME}/scripts/setup-adapters.sh
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^  run_and_time install_folly/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh aws' scripts/setup-ubuntu.sh
    # it's used for velox CI
    sed -i 's/rpm -i minio-20220526054841.0.0.x86_64.rpm/#rpm -i minio-20220526054841.0.0.x86_64.rpm/g' scripts/setup-adapters.sh
  fi
  if [ $ENABLE_GCS == "ON" ]; then
    sed -i '/^  run_and_time install_folly/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh gcs' scripts/setup-ubuntu.sh
  fi
  if [ $ENABLE_ABFS == "ON" ]; then
    sed -i '/^  run_and_time install_folly/a \ \ export AZURE_SDK_DISABLE_AUTO_VCPKG=ON \n '${VELOX_HOME}/scripts'/setup-adapters.sh abfs' scripts/setup-ubuntu.sh
  fi
  sed -i '/run_and_time install_conda/d' scripts/setup-ubuntu.sh

}

function process_setup_centos8 {
  # Allows other version of git already installed.
  if [ -z "$(which git)" ]; then
    dnf install -y -q --setopt=install_weak_deps=False git
  fi
  # make this function Reentrant
  git checkout scripts/setup-centos8.sh
  # need set BUILD_SHARED_LIBS flag for thrift
  sed -i "/cd fbthrift/{n;s/cmake_install -Denable_tests=OFF/cmake_install -Denable_tests=OFF -DBUILD_SHARED_LIBS=OFF/;}" scripts/setup-centos8.sh
  # No need to re-install git.
  sed -i 's/dnf_install ninja-build cmake curl ccache gcc-toolset-9 git/dnf_install ninja-build cmake curl ccache gcc-toolset-9/' scripts/setup-centos8.sh
  sed -i '/^function dnf_install/i\DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}' scripts/setup-centos8.sh
  sed -i '/^dnf_install autoconf/a\dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos8.sh

  if [ $ENABLE_HDFS == "ON" ]; then
    sed -i '/^function install_gflags.*/i function install_libhdfs3 {\n cd "\${DEPENDENCY_DIR}"\n github_checkout oap-project/libhdfs3 master\n cd ..\n cmake_install libhdfs3\n}\n' scripts/setup-centos8.sh
    sed -i '/^  run_and_time install_fbthrift/a \  run_and_time install_libhdfs3' scripts/setup-centos8.sh
    sed -i '/^dnf_install ninja-build/a\ \ yasm \\' scripts/setup-centos8.sh
  fi
  if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
    sed -i '/cd protobuf/{n;s/\.\/configure --prefix=\/usr/\.\/configure CXXFLAGS="-fPIC" --prefix=\/usr\/local/;}' scripts/setup-centos8.sh
  fi
  sed -i "s/yum -y install/sudo yum -y install/" ${VELOX_HOME}/scripts/setup-adapters.sh
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^  run_and_time install_fbthrift/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh aws' scripts/setup-centos8.sh
    # Maybe already installed.
    sed -i 's/rpm -i minio-20220526054841.0.0.x86_64.rpm/rpm -i --replacepkgs minio-20220526054841.0.0.x86_64.rpm/g' scripts/setup-adapters.sh
  fi
  if [ $ENABLE_GCS == "ON" ]; then
    sed -i '/^  run_and_time install_fbthrift/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh gcs' scripts/setup-centos8.sh
  fi
  if [ $ENABLE_ABFS == "ON" ]; then
    sed -i '/^  run_and_time install_fbthrift/a \ \ export AZURE_SDK_DISABLE_AUTO_VCPKG=ON \n '${VELOX_HOME}/scripts'/setup-adapters.sh abfs' scripts/setup-centos8.sh
  fi
}

function process_setup_centos7 {
  # Allows other version of git already installed.
  if [ -z "$(which git)" ]; then
    dnf install -y -q --setopt=install_weak_deps=False git
  fi
  # make this function Reentrant
  git checkout scripts/setup-centos7.sh

  # No need to re-install git.
  sed -i 's/dnf_install ccache git/dnf_install ccache/' scripts/setup-centos7.sh
  # cmake 3 and ninja should be installed
  sed -i '/^run_and_time install_cmake/d' scripts/setup-centos7.sh
  sed -i '/^run_and_time install_ninja/d' scripts/setup-centos7.sh

  # install gtest
  sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_gtest' scripts/setup-centos7.sh

  if [ $ENABLE_HDFS = "ON" ]; then
    sed -i '/^function install_protobuf.*/i function install_libhdfs3 {\n cd "\${DEPENDENCY_DIR}"\n github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-centos7.sh
    sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_libhdfs3' scripts/setup-centos7.sh
    sed -i '/^dnf_install ccache/a\ \ yasm \\' scripts/setup-centos7.sh
  fi
  if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
    sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_protobuf' scripts/setup-centos7.sh
  fi
  sed -i "s/yum -y install/sudo yum -y install/" ${VELOX_HOME}/scripts/setup-adapters.sh
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^  run_and_time install_folly/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh aws' scripts/setup-centos7.sh
    # Maybe already installed.
    sed -i 's/rpm -i minio-20220526054841.0.0.x86_64.rpm/rpm -i --replacepkgs minio-20220526054841.0.0.x86_64.rpm/g' scripts/setup-adapters.sh
  fi
  if [ $ENABLE_GCS == "ON" ]; then
    sed -i '/^  run_and_time install_folly/a \ \ '${VELOX_HOME}/scripts'/setup-adapters.sh gcs' scripts/setup-centos7.sh
  fi
  if [ $ENABLE_ABFS == "ON" ]; then
    sed -i '/^function cmake_install_deps.*/i function install_curl {\n cd "${DEPENDENCY_DIR}" \n github_checkout curl/curl curl-7_68_0 \n cmake -DCMAKE_BUILD_TYPE=Release -DOPENSSL_USE_STATIC_LIBS=TRUE -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON && cmake --build . && sudo cmake --install . \n}\n'     scripts/setup-centos7.sh
    sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_curl' scripts/setup-centos7.sh
    sed -i '/^  run_and_time install_curl/a \ \ export AZURE_SDK_DISABLE_AUTO_VCPKG=ON \n '${VELOX_HOME}/scripts'/setup-adapters.sh abfs' scripts/setup-centos7.sh
  fi
}

function process_setup_alinux3 {
  process_setup_centos8
  sed -i "s/.*dnf_install epel-release/#&/" scripts/setup-centos8.sh
  sed -i "s/.*dnf config-manager --set-enabled powertools/#&/" scripts/setup-centos8.sh
  sed -i "s/gcc-toolset-9 //" scripts/setup-centos8.sh
  sed -i "s/.*source \/opt\/rh\/gcc-toolset-9\/enable/#&/" scripts/setup-centos8.sh
  sed -i "s/\${CMAKE_INSTALL_LIBDIR}/lib64/" third_party/CMakeLists.txt
}

function process_setup_tencentos32 {
  process_setup_centos8
  sed -i "s/.*dnf config-manager --set-enabled powertools/#&/" scripts/setup-centos8.sh
}

echo "Preparing Velox source code..."
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi
VELOX_SOURCE_DIR="${VELOX_HOME}"

# checkout code
TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
if [ -d $VELOX_SOURCE_DIR ]; then
  echo "Velox source folder $VELOX_SOURCE_DIR already exists..."
  cd $VELOX_SOURCE_DIR
  git init .
  EXISTS=$(git show-ref refs/tags/build_$TARGET_BUILD_COMMIT || true)
  if [ -z "$EXISTS" ]; then
    git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:refs/tags/build_$TARGET_BUILD_COMMIT
  fi
  git reset --hard HEAD
  git checkout refs/tags/build_$TARGET_BUILD_COMMIT
else
  git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR
  cd $VELOX_SOURCE_DIR
  git checkout $TARGET_BUILD_COMMIT
fi
#sync submodules
git submodule sync --recursive
git submodule update --init --recursive

function apply_compilation_fixes {
  current_dir=$1
  velox_home=$2
  sudo cp ${current_dir}/modify_velox.patch ${velox_home}/
  sudo cp ${current_dir}/modify_arrow.patch ${velox_home}/third_party/
  git add ${velox_home}/modify_velox.patch # to avoid the file from being deleted by git clean -dffx :/
  git add ${velox_home}/third_party/modify_arrow.patch # to avoid the file from being deleted by git clean -dffx :/
  cd ${velox_home}
  echo "Applying patch to Velox source code..."
  git apply modify_velox.patch
  if [ $? -ne 0 ]; then
    echo "Failed to apply compilation fixes to Velox: $?."
    exit 1
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  # apply patches
  sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
  sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    process_setup_ubuntu
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
      8) process_setup_centos8 ;;
      7) process_setup_centos7 ;;
      *)
        echo "Unsupport centos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
      2) process_setup_centos7 ;;
      3) process_setup_alinux3 ;;
      *)
        echo "Unsupport alinux version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
      3.2) process_setup_tencentos32 ;;
      *)
        echo "Unsupport tencentos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  else
    echo "Unsupport linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}

function setup_macos {
  if [ $ENABLE_HDFS == "ON" ]; then
    echo "Unsupport hdfs"
    exit 1
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    echo "Unsupport s3"
    exit 1
  fi

  sed -i '' $'/^  run_and_time install_double_conversion/a\\\n  run_and_time install_folly\\\n' scripts/setup-macos.sh
  # need set BUILD_SHARED_LIBS flag for thrift
  sed -i '' "/facebook\/fbthrift/{n;s/cmake_install -DBUILD_TESTS=OFF/cmake_install -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF/;}" scripts/setup-macos.sh
}

if [ $OS == 'Linux' ]; then
  setup_linux
elif [ $OS == 'Darwin' ]; then
  setup_macos
else
  echo "Unsupport kernel: $OS"
  exit 1
fi

apply_compilation_fixes $CURRENT_DIR $VELOX_SOURCE_DIR

echo "Velox-get finished."
