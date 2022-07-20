#!/bin/bash

set -eu

NPROC=$(nproc)

BUILD_VELOX_FROM_SOURCE=OFF

for arg in "$@"
do
    case $arg in
        -v=*|--build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	*)
	OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done


echo "Velox Installation"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}


if [ $BUILD_VELOX_FROM_SOURCE == "ON" ]; then
    if [ -d build/velox_ep ]; then
        rm -r build/velox_ep
    fi

    if [ -d build/velox_install ]; then
        rm -r build/velox_install
    fi
    echo "Building Velox from Source ..."
    mkdir -p build
    cd build
    VELOX_PREFIX="${CURRENT_DIR}/build" # Use build directory as VELOX_PREFIX
    VELOX_SOURCE_DIR="${VELOX_PREFIX}/velox_ep"
    VELOX_INSTALL_DIR="${VELOX_PREFIX}/velox_install"

    echo "VELOX_PREFIX=${VELOX_PREFIX}"
    echo "VELOX_SOURCE_DIR=${VELOX_SOURCE_DIR}"
    echo "VELOX_INSTALL_DIR=${VELOX_INSTALL_DIR}"
    mkdir -p $VELOX_SOURCE_DIR
    mkdir -p $VELOX_INSTALL_DIR
    git clone https://github.com/zhejiangxiaomai/velox.git -b arrowStream $VELOX_SOURCE_DIR
    pushd $VELOX_SOURCE_DIR

    #sync submodules
    git submodule sync --recursive
    git submodule update --init --recursive

    sed -i '/libprotobuf-dev/d' scripts/setup-ubuntu.sh
    sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
    sed -i '/^sudo apt install/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
    sed -i 's/^  liblzo2-dev.*/  liblzo2-dev/g' scripts/setup-ubuntu.sh
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    sed -i '/^function install_folly.*/i function install_pb {\n  github_checkout protocolbuffers/protobuf v3.13.0\n  git submodule update --init --recursive\n  ./autogen.sh\n  ./configure CFLAGS=-fPIC CXXFLAGS=-fPIC\n  make -j$(nproc)\n  make check\n  sudo make install\n sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
    sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
    sed -i '/^  run_and_time install_folly/i \ \ run_and_time install_pb' scripts/setup-ubuntu.sh

    scripts/setup-ubuntu.sh
    make release EXTRA_CMAKE_FLAGS=" -DVELOX_ENABLE_PARQUET=ON -DVELOX_ENABLE_ARROW=ON"
    echo "Finish to build Velox from Source !!!"
else
    echo "Use existing Velox."
fi
