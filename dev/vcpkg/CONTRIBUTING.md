# A Quick Guide for Maintaining Dependencies with Vcpkg

It is recommended to read [vcpkg documents](https://learn.microsoft.com/en-us/vcpkg/).
It is quite easy to understand if you have experience in packaging on linux or mac homebrew.

## How to add a new depends?

Please init vcpkg env first:

``` sh
./dev/vcpkg/init.sh
```

Vcpkg already maintains a lot of libraries.
You can find them by vcpkg cli.
(NOTE: Please always use cli because [packages on vcpkg.io](https://vcpkg.io/en/packages.html) is outdate).

```
$ ./.vcpkg/vcpkg search folly
folly                    2022.10.31.00#4  An open-source C++ library developed and used at Facebook. The library is ...
folly[bzip2]                              Support bzip2 for compression
folly[libsodium]                          Support libsodium for cryto
folly[liburing]                           Support compile with liburing
folly[lz4]                                Support lz4 for compression
folly[lzma]                               Support LZMA for compression
folly[snappy]                             Support Snappy for compression
folly[zlib]                               Support zlib for compression
folly[zstd]                               Support zstd for compression
```

`[...]` means additional features. Then add the dependency into [vcpkg.json](./vcpkg.json).

``` json
{
  "velox": {
    "dependencies": [
      {
        "name": "folly",
        "features": ["zstd", "lz4", "lzma", "snappy"]
      }
    ]
  }
}
```

Run `./dev/vcpkg/init.sh` again to verify that everything works.
It will only build modified part.

Sometimes the vcpkg port does not meet the requirements.
You can modify the packaging logic of vcpkg for different cases:

**If vcpkg's port is too new (e.g. fmt)**.
Vcpkg allows you to choose a specific version library.
Supported versions can be found in `.vcpkg/versions/$package`.
Then specify version in `vcpkg.json`.
See also [Versioning](https://learn.microsoft.com/en-us/vcpkg/users/versioning).

``` json
{
  "overrides": [
    { "name": "fmt", "version": "8.0.1" }
  ]
}
```

Otherwise, you must create a new port in `./ports/$package` to override the vcpkg's original version.

**If a new ports has been merged in vcpkg main branch**.
You can find git tree-ish and checkout it.
For example, arrow 12.0.0 has been merged but not include in last release (2023.04.15).

``` patch
# https://patch-diff.githubusercontent.com/raw/microsoft/vcpkg/pull/31321.patch

diff --git a/versions/a-/arrow.json b/versions/a-/arrow.json
index 07c7ef67cb27c..f7bbe94b4f914 100644
--- a/versions/a-/arrow.json
+++ b/versions/a-/arrow.json
@@ -1,5 +1,10 @@
 {
   "versions": [
+    {
+      "git-tree": "881bfaaab349dae46929b36e5b84e7036a009ad3",
+      "version": "12.0.0",
+      "port-version": 0
+    },
     {
       "git-tree": "21fea47a1e9c7bf68e6c088ad5a6b7b6e33c2fcb",
       "version": "11.0.0",
```

Git tree-ish is `21fea47a1e9c7bf68e6c088ad5a6b7b6e33c2fcb`. Then fetch and checkout it.

``` sh
cd .vcpkg
git fetch origin master
git archive 21fea47a1e9c7bf68e6c088ad5a6b7b6e33c2fcb | tar -x -C ../ports/arrow
```

**If you want to modify port based on vcpkg version**.
Copy port directory from `./.vcpkg/ports/$package` to `./ports/$package`.

**If you want to create a new port**.
Create a new directory in `./ports/$package`
**or** generate a template via vcpkg cli:

``` sh
./.vcpkg/vcpkg create $package /url/to/source/or/git
mv ./.vcpkg/ports/$package ./ports/$package
```

### About Boost

Vcpkg splits boost into dozens of packages including header-only libraries.
To find out which boost modules you are using,
the easiest way is to grep all `#include <boost/` in source code.
It is not sufficient to just copy the components from `find_package(Boost)` in cmake scripts.
As a result, modifying the boost version is a bit tricky,
see more [Pin old Boost versions](https://learn.microsoft.com/en-us/vcpkg/users/examples/modify-baseline-to-pin-old-boost).

## Write port files

See also [Packaging GitHub repos example: libogg](https://learn.microsoft.com/en-us/vcpkg/examples/packaging-github-repos).

### Create the manifest file

`vcpkg.json` is a json file describing the package's metadata:

``` json
{
  "name": "gsasl",
  "version": "2.2.0",
  "dependencies": [
    "krb5",
    "libntlm"
  ]
}
```

See [vcpkg.json reference](https://learn.microsoft.com/en-us/vcpkg/reference/vcpkg-json);

### Create the portfile (aka build script)

`portfile.cmake` is a cmake script describing how to build and install the package.
A typical portfile has 3 stages:

**Download and prepare source**:

``` cmake
# Download from Github
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO gflags/gflags
    REF v2.2.2
    SHA512 98c4703aab24e81fe551f7831ab797fb73d0f7dfc516addb34b9ff6d0914e5fd398207889b1ae555bac039537b1d4677067dae403b64903577078d99c1bdb447
    HEAD_REF master
    PATCHES
        0001-patch-dir.patch # gflags was estimating a wrong relative path between the gflags-config.cmake file and the include path; "../.." goes from share/gflags/ to the triplet root
        fix_cmake_config.patch
)

# Download from source archive
vcpkg_download_distfile(ARCHIVE
    URLS "https://ftp.gnu.org/gnu/gsasl/gsasl-2.2.0.tar.gz"
    FILENAME "gsasl-2.2.0.tar.gz"
    SHA512 0ae318a8616fe675e9718a3f04f33731034f9a7ba03d83ccb1a72954ded54ced35dc7c7e173fdcb6fa0f0813f8891c6cbcedf8bf70b37d00b8ec512eb9f07f5f
)
vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES fix-krb5-config.patch
)
```

**Configure, build and install**. Vcpkg will add extra flags (e.g. `--install-prefix`) for common build system automatically.

``` cmake
# CMake
vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        -DCMAKE_PROGRAM_PATH=${CURRENT_HOST_INSTALLED_DIR}/tools/yasm
        -DWITH_KERBEROS=on
)
vcpkg_install_cmake()

# GNU Autotools (Out of tree)
vcpkg_configure_make(SOURCE_PATH ${SOURCE_PATH} AUTOCONFIG)
vcpkg_install_make()

# Meson
vcpkg_configure_meson(SOURCE_PATH ${SOURCE_PATH})
vcpkg_install_meson()
```

Some libraries do not support out-of-tree build. There is a hidden option for `vcpkg_configure_make()`:

```
vcpkg_configure_make(
    SOURCE_PATH ${SOURCE_PATH}
    COPY_SOURCE
)
```

**Post patch**. 
You need to modify the final package in order to
make the binary library work in any location.
Vcpkg will also validate the final package and warn common issues.

``` cmake
# Remove unused files
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

# Replace absolute path in pkgconfig
vcpkg_fixup_pkgconfig()

# Missing copyright
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
```

See portfile [variable](https://learn.microsoft.com/en-us/vcpkg/maintainers/variables)
and [function](https://learn.microsoft.com/en-us/vcpkg/maintainers/functions/vcpkg_acquire_msys) reference.

### Optional Files

* `usage`: A txt file about how to use the package. See [example: gflags](./ports/gflags)
* `vcpkg-cmake-wrapper.cmake`: A cmake script to wrapper library's find module.
  If exists, `find_package()` will exec it instead of `Find*.cmake`.
  See [example: gflags](./ports/gflags)

### Package Layout

Build intermediate files can be found in `.vcpkg/buildtrees/$package`.
Built packages can be found in `.vcpkg/$package`:

```
packages/thrift_x64-linux-avx/
├── BUILD_INFO
├── CONTROL
├── debug
│   └── lib
│       ├── libthriftd.a
│       ├── ...
│       └── pkgconfig
│           ├── thrift.pc
│           └── ...
├── include
│   └── thrift
│       ├── Thrift.h
│       └── ...
├── lib
│   ├── libthrift.a
│   ├── ...
│   └── pkgconfig
│       ├── thrift.pc
│       └── ...
├── share
│   └── thrift
│       ├── copyright
│       ├── ThriftConfig.cmake
│       ├── ...
│       ├── vcpkg_abi_info.txt
│       └── vcpkg.spdx.json
└── tools
    └── thrift
        └── thrift
```

Most of them are same as the common linux packages layout, except:

* `debug/lib`: debug-build libraries
* `share/$package`: cmake module (aka `/usr/lib/cmake/$package`), copyright, usage info, vcpkg abi (hash of build scripts).
* `tools/$package`: executables (aka `/bin`)

## About Binary Cache

> Binary caching saves copies of library binaries in a shared location that can be accessed by vcpkg for future installation.
> This means that, as a user, you should only need to build dependencies from source once. If vcpkg is asked to install the
> same library with the same build configuration in the future, it will copy the built binaries from the cache and finish the operation in seconds.

Binary cache is enabled by default. Cache files is located in `~/.cache/vcpkg/archives`. See [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching).
