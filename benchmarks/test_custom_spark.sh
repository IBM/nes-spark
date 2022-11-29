#!/usr/bin/env bash
set -euo pipefail

# Set the benchmark root folder
ROOT=$(cd `dirname $0` && pwd)
cd $ROOT

# Number of iterations for each test
NUM_ITERATIONS=1

# Number of executors 
NUM_EXECUTORS=20

# Current timestamp
TIMESTAMP=$(date -u "+%FT%H%M%SZ")

# Get parameters from user defined file
SPARK_VERSION="$(cat params.json | jq -r '.spark_version')"
DOCKER_REPO="$(cat params.json | jq -r '.docker_repo')"
DOCKER_SPARK_IMG="$(cat params.json | jq -r '.docker_spark_img')"
DOCKER_SPARK_IMG_TAG="$(cat params.json | jq -r '.docker_spark_img_tag')"
PREFIX="workload-custom-"

function getImageName() {
    local image=$1
    echo "${DOCKER_REPO}/${PREFIX}${image}:${SPARK_VERSION}"
}

benchmark='spark-sql-benchmark'
# benchmark='spark-tpch'
echo "Running $benchmark.."

# Define Spark related parameters (assuming homogeneous set of executors)
export DRIVER_CPU=4
export DRIVER_MEM=13000M
export DRIVER_MEMORY_OVERHEAD=3000M
export EXECUTOR_CPU=2
export EXECUTOR_MEM=13000M
export EXECUTOR_MEMORY_OVERHEAD=3000M
export IMAGE=$(getImageName $benchmark)

export SIZE=100
export INSTANCES=$NUM_EXECUTORS

# Define query to run (in the form qN where N is an integer) 
QUERY=q67

if [[ $benchmark == "spark-tpch" ]]; then
    name="tpch-sql"
else
    name="tpcds-sql"
fi
# Run each query of the subset iteratively NUM_ITERATIONS times 
miniomc mb minio/$MINIO_OUTPUT_BUCKET/k8s
miniomc mb minio/$MINIO_LOGS_BUCKET/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}
miniomc mb minio/$MINIO_OUTPUT_BUCKET/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}


for ((i=0; i < NUM_ITERATIONS; i++)); do
    export PROCESS_TAG="${TIMESTAMP}-${QUERY}-sf${SIZE}-ex${INSTANCES}-it${i}"
    if [[ $benchmark == "spark-tpch" ]]; then
        ./docker/${benchmark}/test_run.sh \
            --query $QUERY \
            --dataset-base-location ${INPUT_DATA_PREFIX}/sf${SIZE}/ \
            --result-location ${OUTPUT_DATA_PREFIX}/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}/${PROCESS_TAG} || true &
    else
        ./docker/${benchmark}/test_run.sh \
            -t $QUERY \
            -i ${INPUT_DATA_PREFIX}/sf${SIZE}_parquet/ \
            -a save,${OUTPUT_DATA_PREFIX}/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}/${PROCESS_TAG} || true &
    fi
    # Wait for the driver pod to start
    out=$(kubectl get pods 2> /dev/null)
    array=($out)
    while [ ${#array[@]} -lt 10 ]; do
        out=$(kubectl get pods 2> /dev/null)
        array=($out)
    done
    driver_pod=$( echo $out | awk '{print $6}')
    echo $driver_pod
    status=${array[7]}
    while [ $status != 'Running' ]; do
        out=$(kubectl get pods 2> /dev/null)
        array=($out)
        status=${array[7]}
    done 

    kubectl logs --follow $driver_pod &> driver_logs.logs
    driver_log_fname=$(miniomc ls minio/$MINIO_LOGS_BUCKET/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS} | sort -t. -k 1.1,1.19 | tail -1 | awk '{print $6}')
    driver_log_fname=$driver_log_fname.log
    cp driver_logs.logs $driver_log_fname
    miniomc cp $driver_log_fname minio/$MINIO_DLOGS_BUCKET/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}/
    kubectl delete pod $driver_pod
    rm $driver_log_fname
done
#kubectl -n $K8S_NAMESPACE delete --all pods

# Delete all k8s/spark temporary files
miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/k8s/
# Delete all output files 
miniomc rm --recursive --force minio/$MINIO_OUTPUT_BUCKET/test-custom-spark-${name}-${QUERY}-sf${SIZE}-ex${NUM_EXECUTORS}

