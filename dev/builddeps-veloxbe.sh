#!/bin/bash
####################################################################################################
#  The main function of this script is to allow developers to build the environment with one click #
#  Recommended commands for first-time installation:                                               #
#  ./dev/buildbundle-veloxbe.sh                                                            #
####################################################################################################
set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
BUILD_TYPE=Release
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=OFF
BUILD_PROTOBUF=ON
BUILD_VELOX_TESTS=OFF
BUILD_VELOX_BENCHMARKS=OFF
ENABLE_QAT=OFF
ENABLE_IAA=OFF
ENABLE_HBM=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF
ENABLE_EP_CACHE=OFF
ARROW_ENABLE_CUSTOM_CODEC=OFF
ENABLE_VCPKG=OFF
RUN_SETUP_SCRIPT=ON
VELOX_REPO=""
VELOX_BRANCH=""
VELOX_HOME=""
VELOX_PARAMETER=""
COMPILE_ARROW_JAVA=OFF

# set default number of threads as cpu cores minus 2
if [[ "$(uname)" == "Darwin" ]]; then
    physical_cpu_cores=$(sysctl -n hw.physicalcpu)
    ignore_cores=2
    if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
        NUM_THREADS=${NUM_THREADS:-$(($physical_cpu_cores - $ignore_cores))}
    else
        NUM_THREADS=${NUM_THREADS:-$physical_cpu_cores}
    fi
else
    NUM_THREADS=${NUM_THREADS:-$(nproc --ignore=2)}
fi

for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_tests=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_examples=*)
        BUILD_EXAMPLES=("${arg#*=}")
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
        --enable_qat=*)
        ENABLE_QAT=("${arg#*=}")
        ARROW_ENABLE_CUSTOM_CODEC=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_iaa=*)
        ENABLE_IAA=("${arg#*=}")
        ARROW_ENABLE_CUSTOM_CODEC=("${arg#*=}")
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
        --enable_gcs=*)
        ENABLE_GCS=("${arg#*=}")
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
        --enable_abfs=*)
        ENABLE_ABFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_ep_cache=*)
        ENABLE_EP_CACHE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_vcpkg=*)
        ENABLE_VCPKG=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --run_setup_script=*)
        RUN_SETUP_SCRIPT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
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
        --build_velox_tests=*)
        BUILD_VELOX_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_benchmarks=*)
        BUILD_VELOX_BENCHMARKS=("${arg#*=}")
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

function concat_velox_param {
    # check velox repo
    if [[ -n $VELOX_REPO ]]; then
        VELOX_PARAMETER+="--velox_repo=$VELOX_REPO "
    fi

    # check velox branch
    if [[ -n $VELOX_BRANCH ]]; then
        VELOX_PARAMETER+="--velox_branch=$VELOX_BRANCH "
    fi

    # check velox home
    if [[ -n $VELOX_HOME ]]; then
        VELOX_PARAMETER+="--velox_home=$VELOX_HOME "
    fi
}

if [ "$ENABLE_VCPKG" = "ON" ]; then
    # vcpkg will install static depends and init build environment
    envs="$("$GLUTEN_DIR/dev/vcpkg/init.sh")"
    eval "$envs"
fi

## build velox
concat_velox_param
cd $GLUTEN_DIR/ep/build-velox/src
./get_velox.sh --enable_hdfs=$ENABLE_HDFS --build_protobuf=$BUILD_PROTOBUF --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS --enable_abfs=$ENABLE_ABFS $VELOX_PARAMETER
# When BUILD_TESTS is on for gluten cpp, we need turn on VELOX_BUILD_TEST_UTILS via build_test_utils.
./build_velox.sh --run_setup_script=$RUN_SETUP_SCRIPT --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS --build_type=$BUILD_TYPE --enable_hdfs=$ENABLE_HDFS \
                 --enable_abfs=$ENABLE_ABFS --enable_ep_cache=$ENABLE_EP_CACHE --build_test_utils=$BUILD_TESTS --build_tests=$BUILD_VELOX_TESTS --build_benchmarks=$BUILD_VELOX_BENCHMARKS \
                 --compile_arrow_java=$COMPILE_ARROW_JAVA  --num_threads=$NUM_THREADS

## compile gluten cpp
cd $GLUTEN_DIR/cpp
rm -rf build
mkdir build
cd build
cmake -DBUILD_VELOX_BACKEND=ON -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
      -DBUILD_TESTS=$BUILD_TESTS -DBUILD_EXAMPLES=$BUILD_EXAMPLES -DBUILD_BENCHMARKS=$BUILD_BENCHMARKS -DBUILD_JEMALLOC=$BUILD_JEMALLOC \
      -DENABLE_HBM=$ENABLE_HBM -DENABLE_QAT=$ENABLE_QAT -DENABLE_IAA=$ENABLE_IAA -DENABLE_GCS=$ENABLE_GCS -DENABLE_S3=$ENABLE_S3 -DENABLE_HDFS=$ENABLE_HDFS -DENABLE_ABFS=$ENABLE_ABFS ..
make -j $NUM_THREADS
