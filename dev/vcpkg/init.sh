#! /bin/bash

set -e

exec 3>&1 >&2

SCRIPT_ROOT="$(realpath "$(dirname "$0")")"
VCPKG_ROOT="$SCRIPT_ROOT/.vcpkg"
VCPKG="$SCRIPT_ROOT/.vcpkg/vcpkg"
VCPKG_TRIPLET=x64-linux-avx

cd "$SCRIPT_ROOT"

if [ ! -d "$VCPKG_ROOT" ] || [ -z "$(ls "$VCPKG_ROOT")" ]; then
    git clone https://github.com/microsoft/vcpkg.git --branch 2023.10.19 "$VCPKG_ROOT"
fi
[ -f "$VCPKG" ] || "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

$VCPKG install --no-print-usage \
    --triplet="${VCPKG_TRIPLET}" --host-triplet="${VCPKG_TRIPLET}"

VCPKG_TRIPLET_INSTALL_DIR=${SCRIPT_ROOT}/vcpkg_installed/${VCPKG_TRIPLET}
EXPORT_TOOLS_PATH=
EXPORT_TOOLS_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/tools/protobuf:${EXPORT_TOOLS_PATH}"

# This scripts depends on environment $CMAKE_TOOLCHAIN_FILE, which requires
# cmake >= 3.21. If system cmake < 3.25, vcpkg will download latest cmake. We
# can use vcpkg's internal cmake if we find it.
VCPKG_CMAKE_BIN_DIR=$(echo "${VCPKG_ROOT}"/downloads/tools/cmake-*/cmake-*/bin)
if [ -f "$VCPKG_CMAKE_BIN_DIR/cmake" ]; then
    EXPORT_TOOLS_PATH="${VCPKG_CMAKE_BIN_DIR}:${EXPORT_TOOLS_PATH}"
fi

EXPORT_TOOLS_PATH=${EXPORT_TOOLS_PATH/%:/}

# For fixing a build error like below when gluten's build type is Debug:
# No rule to make target '/root/gluten/dev/vcpkg/vcpkg_installed/x64-linux-avx/debug/lib/libz.a',
# needed by 'releases/libvelox.so'
mkdir -p $VCPKG_TRIPLET_INSTALL_DIR/debug/lib/
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libz.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libssl.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libcrypto.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/liblzma.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libdwarf.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libhdfs3.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib

cat <<EOF >&3
if [ "\${GLUTEN_VCPKG_ENABLED:-}" != "${VCPKG_ROOT}" ]; then
    export VCPKG_ROOT=${VCPKG_ROOT}
    export VCPKG_MANIFEST_DIR=${SCRIPT_ROOT}
    export VCPKG_TRIPLET=${VCPKG_TRIPLET}

    export CMAKE_TOOLCHAIN_FILE=${SCRIPT_ROOT}/toolchain.cmake
    export PKG_CONFIG_PATH=${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${VCPKG_TRIPLET_INSTALL_DIR}/share/pkgconfig:\${PKG_CONFIG_PATH:-}
    export PATH="${EXPORT_TOOLS_PATH}:\$PATH"

    export GLUTEN_VCPKG_ENABLED=${VCPKG_ROOT}
else
    echo "Gluten's vcpkg environment is enabled" >&2
fi
EOF
