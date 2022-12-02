#!/bin/bash
####################################################################################################
#  The main function of this script is to allow developers to build the environment with one click #
#  Recommended commands for first-time installation:                                               #
#  ./tools/one_step_veloxbackend.sh                                                                #
####################################################################################################
set -exu
GLUTEN_DIR=`pwd`
BUILD_TYPE=release
BUILD_TESTS=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=ON
ENABLE_HBM=OFF
BUILD_PROTOBUF=ON
ENABLE_S3=OFF
ENABLE_HDFS=OFF
BUILD_FOLLY=ON
BUILD_ARROW_FROM_SOURCE=ON
BUILD_VELOX_FROM_SOURCE=ON

VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=main
ARROW_REPO=https://github.com/oap-project/arrow.git
ARROW_BRANCH=backend_velox_main

for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_test=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_benchmarks=*)
        BUILD_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_jemalloc=*)
        BUILD_JEMALLOC=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hbm=*)
        ENABLE_HBM=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_protobuf=*)
        BUILD_PROTOBUF=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_s3=*)
        ENABLE_S3=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hdfs=*)
        ENABLE_HDFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_folly=*)
        BUILD_FOLLY=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_arrow_from_source=*)
        BUILD_ARROW_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	    *)
	    OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

##install arrow
cd $GLUTEN_DIR/ep/build_arrow/src
if [ $BUILD_ARROW_FROM_SOURCE == "ON" ]; then
  ./get_arrow.sh --gluten_dir=$GLUTEN_DIR --arrow_repo=$ARROW_REPO --arrow_branch=$ARROW_BRANCH
fi
./build_arrow_for_velox.sh  --gluten_dir=$GLUTEN_DIR --build_type=$BUILD_TYPE --build_test=$BUILD_TESTS \
                            --build_benchmarks=$BUILD_BENCHMARKS

##install velox
cd $GLUTEN_DIR/ep/build-velox/src
if [ $BUILD_VELOX_FROM_SOURCE == "ON" ]; then
  ./get_velox.sh --gluten_dir=$GLUTEN_DIR --velox_repo=$VELOX_REPO --velox_branch=$VELOX_BRANCH
fi
./build_velox.sh --gluten_dir=$GLUTEN_DIR --build_protobuf=$BUILD_PROTOBUF --build_folly=$BUILD_FOLLY \
                 --build_type=$BUILD_TYPE --enable_hdfs=$ENABLE_HDFS  --build_type=$BUILD_TYPE
                 --enable_s3

## compile gluten cpp
cd $GLUTEN_DIR/cpp
./compile.sh --build_velox_backend=ON --gluten_dir=$GLUTEN_DIR --build_type=$BUILD_TYPE --build_velox_backend=ON \
             --build_test=$BUILD_TESTS --build_benchmarks=$BUILD_BENCHMARKS --build_jemalloc=$BUILD_JEMALLOC \
             --enable_hbm=$ENABLE_HBM --enable_s3=$ENABLE_S3 --enable_hdfs=$ENABLE_HDFS

cd $GLUTEN_DIR
mvn clean package -Pbackends-velox -Pspark-3.2