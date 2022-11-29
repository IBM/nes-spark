#!/usr/bin/env bash
set -euo pipefail

# Set the benchmark root folder
ROOT=$(cd `dirname $0` && pwd)
cd $ROOT

# Number of iterations for each test
NUM_ITERATIONS=5

# Number of executors 
NUM_EXECUTORS=( 
    1
    10
    20
    30
    40
    50
)

# Current timestamp
TIMESTAMP=$(date -u "+%FT%H%M%SZ")

# Get parameters from user defined file
SPARK_VERSION="$(cat params.json | jq -r '.spark_version')"
DOCKER_REPO="$(cat params.json | jq -r '.docker_repo')"
DOCKER_SPARK_IMG="$(cat params.json | jq -r '.docker_spark_img')"
DOCKER_SPARK_IMG_TAG="$(cat params.json | jq -r '.docker_spark_img_tag')"
PREFIX="workload-custom-"

# Set tests images names
IMAGES=(
    spark-tpcds
    spark-tpch
    spark-sql-benchmark
)

# Global variables defining the operation to perform
BUILD=0
BENCHMARK=0
DYNAMIC=0

# Global variable defining the benchmark to test (default is all)
BENCHMARK_NAME="all"

# Explain script usage
function usage(){
    echo "Usage: $0 --build --benchmark --name banchmark_name"
    echo -e "\t--build \tBuild benchmark(s) docker image(s)"
    echo -e "\t--benchmark \tRun the benchmark(s)"
    echo -e "\t--dynamic \tEnable dynamic allocation"
    echo -e "\t--name  \tBenchmark name (tpcds, tpch, or sql), default 'all'"
    exit 1
}

# Check parameters 
function get_paramenters(){
    # Check if at least one paramenter is given
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi
    # Parse the given paramenters
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            --build)
                BUILD=1
                shift 
                ;;
            --benchmark)
                BENCHMARK=1
                shift
                ;;
            --dynamic)
                DYNAMIC=1
                shift
                ;;
            --name)
                BENCHMARK_NAME="$2"
                shift
                if [[ $BENCHMARK_NAME != "all" ]]; then
                    if [[ ( $BENCHMARK_NAME != "tpcds" ) && ( $BENCHMARK_NAME != "sql" ) && ( $BENCHMARK_NAME != "tpch" )  ]]; then
                        echo "ERROR: Benchmark name can only be tpcds and sql!"
                        usage
                        exit 1
                    fi
                fi
                shift
                ;;
            --help|*)
                usage
                exit 1 
                ;;
        esac
    done
    # Check if either build or benchmark option is given
    if [[ $(($BUILD + $BENCHMARK)) == 0 ]]; then
        echo "ERROR: One option between build and benchmark has to be specified!"
        usage
        exit 1
    fi 
}

function getImageName() {
    local image=$1
    echo "${DOCKER_REPO}/${PREFIX}${image}:${SPARK_VERSION}"
}

function createImage() {
    local image=$1
    local dockerTarget=$(getImageName $image)
    echo "Building $image: ${dockerTarget}.."
    docker build \
        -t "${dockerTarget}" \
        -f "docker/${image}/Dockerfile" \
        --build-arg=REPO=${DOCKER_REPO} \
        --build-arg=IMAGE=${DOCKER_SPARK_IMG} \
        --build-arg=TAG=${DOCKER_SPARK_IMG_TAG} .
    docker push "${dockerTarget}"
}

# Run the whole TCP-DS benchmark
function runTPCDS() {
    benchmark=$1
    echo "Running $benchmark.."
    # Define Spark related parameters (assuming homogeneous set of executors)
    export DRIVER_CPU=4
    export DRIVER_MEM=13000M
    export DRIVER_MEMORY_OVERHEAD=3000M
    export EXECUTOR_CPU=2
    export EXECUTOR_MEM=13000M
    export EXECUTOR_MEMORY_OVERHEAD=3000M
    export INSTANCES=4
    export IMAGE=$(getImageName $benchmark)
    export SIZE=100
    # Run each query of the subset iteratively N times 
    for ((i=0; i < NUM_ITERATIONS; i++)); do
        export NUM_ITERATIONS=${NUM_ITERATIONS:-1}
        export PROCESS_TAG="${TIMESTAMP}-${NUM_ITERATIONS}iter_t${i}"
        ./docker/${benchmark}/run.sh
    done
}

# Run a defined subset of queries belongin to TPC-DS benchmark
function runSQLBenchmark() {
    benchmark=$1
    echo "Running $benchmark.."
    # Define Spark related parameters (assuming homogeneous set of executors)
    export DRIVER_CPU=4
    export DRIVER_MEM=13000M
    export DRIVER_MEMORY_OVERHEAD=3000M
    export EXECUTOR_CPU=2
    export EXECUTOR_MEM=13000M
    export EXECUTOR_MEMORY_OVERHEAD=3000M
    export IMAGE=$(getImageName $benchmark)
    # Define subset of queries to run (in the form qN where N is an integer) 
    QUERIES=(
        q67
        #q72
    )
    
    SIZES=( 100 )

    # Run each query of the subset iteratively NUM_ITERATIONS times 
    for size in "${SIZES[@]}"; do
        export SIZE=$size
        for query in "${QUERIES[@]}"; do
            for num in "${NUM_EXECUTORS[@]}"; do
                export INSTANCES=$num
                folder=tpcds-sql-${query}-sf${SIZE}-ex${num}-test
                miniomc mb minio/$MINIO_OUTPUT_BUCKET/k8s
                miniomc mb minio/$MINIO_LOGS_BUCKET/$folder
                miniomc mb minio/$MINIO_OUTPUT_BUCKET/$folder
                for ((i=0; i < NUM_ITERATIONS; i++)); do
                    export PROCESS_TAG="${TIMESTAMP}-${query}-sf${SIZE}-ex${INSTANCES}-it${i}"
                    ./docker/${benchmark}/run.sh \
                        -t $query \
                        -i ${INPUT_DATA_PREFIX}/sf${SIZE}_parquet/ \
                        -a save,${OUTPUT_DATA_PREFIX}/${folder}/${PROCESS_TAG} || true
                    out=$(kubectl get pods 2> /dev/null) 
                    driver_pod=$( echo $out | awk '{print $6}')
                    driver_log_fname=$(miniomc ls minio/$MINIO_LOGS_BUCKET/$folder | sort -t. -k 1.1,1.19 | tail -1 | awk '{print $6}')
                    driver_log_fname=$driver_log_fname.log
                    kubectl logs $driver_pod > $driver_log_fname
                    echo "Dumped driver logs to $driver_log_fname"
                    miniomc cp $driver_log_fname minio/$MINIO_DLOGS_BUCKET/$folder/
                    kubectl delete pod $driver_pod
                    rm $driver_log_fname
                done
                #kubectl -n $K8S_NAMESPACE delete --all pods
            done
        done
    done
    # Delete all k8s/spark temporary files
    miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/k8s/
    # Delete all output files 
    miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/$folder
}

# Run a defined subset of queries belongin to TPC-DS benchmark
function runSQLBenchmarkDynamic() {
    benchmark=$1
    echo "Running $benchmark with dynamic allocation.."
    # Define Spark related parameters (assuming homogeneous set of executors)
    export DRIVER_CPU=4
    export DRIVER_MEM=13000M
    export DRIVER_MEMORY_OVERHEAD=3000M
    export EXECUTOR_CPU=2
    export EXECUTOR_MEM=13000M
    export EXECUTOR_MEMORY_OVERHEAD=3000M
    export IMAGE=$(getImageName $benchmark)
    export MIN_EXECUTORS=1
    export MAX_EXECUTORS=10
    # Define subset of queries to run (in the form qN where N is an integer) 
    QUERIES=(
        q67
        #q72
    )
    
    SIZES=( 100 )

    # Run each query of the subset iteratively NUM_ITERATIONS times 
    for size in "${SIZES[@]}"; do
        export SIZE=$size
        for query in "${QUERIES[@]}"; do
            folder=tpcds-sql-${query}-sf${SIZE}-dyn${MIN_EXECUTORS}to${MAX_EXECUTORS}-test
            miniomc mb minio/$MINIO_OUTPUT_BUCKET/k8s
            miniomc mb minio/$MINIO_LOGS_BUCKET/$folder
            miniomc mb minio/$MINIO_OUTPUT_BUCKET/$folder
            for ((i=0; i < NUM_ITERATIONS; i++)); do
                export PROCESS_TAG="${TIMESTAMP}-${query}-sf${SIZE}-dyn${MIN_EXECUTORS}to${MAX_EXECUTORS}-it${i}"
                ./docker/${benchmark}/run_dyn.sh \
                    -t $query \
                    -i ${INPUT_DATA_PREFIX}/sf${SIZE}_parquet/ \
                    -a save,${OUTPUT_DATA_PREFIX}/$folder/${PROCESS_TAG} || true
                out=$(kubectl get pods 2> /dev/null) 
                driver_pod=$( echo $out | awk '{print $6}')
                driver_log_fname=$(miniomc ls minio/$MINIO_LOGS_BUCKET/$folder | sort -t. -k 1.1,1.19 | tail -1 | awk '{print $6}')
                driver_log_fname=$driver_log_fname.log
                kubectl logs $driver_pod > $driver_log_fname
                miniomc cp $driver_log_fname minio/$MINIO_DLOGS_BUCKET/$folder/
                kubectl delete pod $driver_pod
                rm $driver_log_fname
            done
        done
    done
    # Delete all k8s/spark temporary files
    miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/k8s/
    # Delete all output files 
    miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/$folder
}

# Run a defined subset of queries belonging to TPC-H benchmark
function runTPCHQueries() {
    benchmark=$1
    # Define Spark related parameters (assuming homogeneous set of executors)
    export SIZE=1
    export DRIVER_CPU=4
    export DRIVER_MEM=13000M
    export DRIVER_MEMORY_OVERHEAD=3000M
    export EXECUTOR_CPU=2
    export EXECUTOR_MEM=13000M
    export EXECUTOR_MEMORY_OVERHEAD=3000M
    export IMAGE=$(getImageName $benchmark)
    # Define subset of queries to run (in the form qN where N is an integer)
    QUERIES=( q9 
    )
    SIZES=( 100 )
    
    # Run each query of the subset iteratively N times 
    for size in "${SIZES[@]}"; do
        export SIZE=$size
        for query in "${QUERIES[@]}"; do
            for num in "${NUM_EXECUTORS[@]}"; do
                export INSTANCES=$num
                echo "Running query $query with $num executors on sf$SIZE.."
                folder=tpch-${query}-sf${SIZE}-ex${num}-test
                miniomc mb minio/$MINIO_OUTPUT_BUCKET/k8s
                miniomc mb minio/$MINIO_LOGS_BUCKET/$folder
                miniomc mb minio/$MINIO_OUTPUT_BUCKET/$folder
                for ((i=0; i < NUM_ITERATIONS; i++)); do
                    export PROCESS_TAG="${TIMESTAMP}-${query}-sf${SIZE}-ex${INSTANCES}-it${i}"
                    ./docker/${benchmark}/run.sh \
                        --query $query \
                        --dataset-base-location ${INPUT_DATA_PREFIX}/sf${SIZE}/ \
                        --result-location ${OUTPUT_DATA_PREFIX}/${folder}/${PROCESS_TAG} || true
                    out=$(kubectl get pods 2> /dev/null) 
                    driver_pod=$( echo $out | awk '{print $6}')
                    driver_log_fname=$(miniomc ls minio/$MINIO_LOGS_BUCKET/${folder} | sort -t. -k 1.1,1.19 | tail -1 | awk '{print $6}')
                    driver_log_fname=$driver_log_fname.log
                    kubectl logs $driver_pod > $driver_log_fname
                    miniomc cp $driver_log_fname minio/$MINIO_DLOGS_BUCKET/$folder/
                    kubectl delete pod $driver_pod
                    rm $driver_log_fname
                done
            done
        done
    done
    if [ ${#QUERIES[@]} -eq 0 ]; then
        for ((q=1; q<=22; q++)); do
            query="q$q"
            for num in ${NUM_EXECUTORS}; do
                export INSTANCES=$num
                echo "Running query $query with $num executors on sf$SIZE.."
                miniomc mb minio/$MINIO_LOGS_BUCKET/tpch-${query}-sf${SIZE}-ex${num}
                miniomc mb minio/$MINIO_OUTPUT_BUCKET/tpch-${query}-sf${SIZE}-ex${num}
                for ((i=0; i < NUM_ITERATIONS; i++)); do
                    export PROCESS_TAG="${TIMESTAMP}-${query}-sf${SIZE}-ex${INSTANCES}-it${i}"
                    ./docker/${benchmark}/run.sh \
                        --query $query \
                        --dataset-base-location ${INPUT_DATA_PREFIX}/sf${SIZE}/ \
                        --result-location ${OUTPUT_DATA_PREFIX}/tpch-${query}-sf${SIZE}-ex${num}/${PROCESS_TAG} || true
                done
            done 
        done
    fi
}

# Run a defined subset of queries belonging to TPC-H benchmark
function runTPCHQueriesDynamic() {
    benchmark=$1
    # Define Spark related parameters (assuming homogeneous set of executors)
    export SIZE=1
    export DRIVER_CPU=4
    export DRIVER_MEM=13000M
    export DRIVER_MEMORY_OVERHEAD=3000M
    export EXECUTOR_CPU=2
    export EXECUTOR_MEM=13000M
    export EXECUTOR_MEMORY_OVERHEAD=3000M
    export IMAGE=$(getImageName $benchmark)
    export MIN_EXECUTORS=1
    export MAX_EXECUTORS=50
    # Define subset of queries to run (in the form qN where N is an integer)
    QUERIES=( q9 
    )
    SIZES=( 100 )
    
    # Run each query of the subset iteratively N times 
    for size in "${SIZES[@]}"; do
        export SIZE=$size
        for query in "${QUERIES[@]}"; do
            echo "Running query $query with $MAX_EXECUTORS dynamic executors on sf$SIZE.."
            folder=tpch-${query}-sf${SIZE}-dyn${MIN_EXECUTORS}to${MAX_EXECUTORS}-test
            miniomc mb minio/$MINIO_OUTPUT_BUCKET/k8s
            miniomc mb minio/$MINIO_LOGS_BUCKET/$folder
            miniomc mb minio/$MINIO_OUTPUT_BUCKET/$folder
            for ((i=0; i < NUM_ITERATIONS; i++)); do
                export PROCESS_TAG="${TIMESTAMP}-${query}-sf${SIZE}-dyn${MIN_EXECUTORS}to${MAX_EXECUTORS}-it${i}"
                ./docker/${benchmark}/run_dyn.sh \
                    --query $query \
                    --dataset-base-location ${INPUT_DATA_PREFIX}/sf${SIZE}/ \
                    --result-location ${OUTPUT_DATA_PREFIX}/${folder} || true
                out=$(kubectl get pods 2> /dev/null) 
                driver_pod=$( echo $out | awk '{print $6}')
                driver_log_fname=$(miniomc ls minio/$MINIO_LOGS_BUCKET/${folder} | sort -t. -k 1.1,1.19 | tail -1 | awk '{print $6}')
                driver_log_fname=$driver_log_fname.log
                kubectl logs $driver_pod > $driver_log_fname
                miniomc cp $driver_log_fname minio/${MINIO_DLOGS_BUCKET}/${folder}/
                kubectl delete pod $driver_pod
                rm $driver_log_fname
            done
        done
    done
}

# Get paramenters 
get_paramenters $@

# Build benchmark(s) image(s) if required
if [[ $BUILD -eq 1 ]]; then
    echo "------------- BUILD IMAGE(S) -------------"
    if [[ ( $BENCHMARK_NAME == "all" ) || ( "${BENCHMARK_NAME}" == "tpcds" ) ]]; then
        createImage ${IMAGES[0]}
    fi
    if [[ ( $BENCHMARK_NAME == "all" ) || ( "${BENCHMARK_NAME}" == "tpch" ) ]]; then
        createImage ${IMAGES[1]}
    fi
    if [[ ( $BENCHMARK_NAME == "all" ) || ( $BENCHMARK_NAME == "sql" ) ]]; then
        createImage ${IMAGES[2]}
    fi
fi

# Run benchmark(s) if required
if [[ $BENCHMARK -eq 1 ]]; then
    echo "------------- RUN BENCHMARK(S) -------------"
    if [[ ( $BENCHMARK_NAME == "all" ) || ( $BENCHMARK_NAME == "tpcds" ) ]]; then
        runTPCDS ${IMAGES[0]}
    fi
    if [[ ( $BENCHMARK_NAME == "all" ) || ( $BENCHMARK_NAME == "tpch" ) ]]; then
        if [[ $DYNAMIC == 0 ]]; then
            runTPCHQueries ${IMAGES[1]}
        else
            runTPCHQueriesDynamic ${IMAGES[1]}
        fi
    fi
    if [[ ( $BENCHMARK_NAME == "all" ) || ( $BENCHMARK_NAME == "sql" ) ]]; then
        if [[ $DYNAMIC == 0 ]]; then
            runSQLBenchmark ${IMAGES[2]}
        else
            runSQLBenchmarkDynamic ${IMAGES[2]}
        fi
    fi
fi
