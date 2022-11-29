#!/usr/bin/env bash
set -euo pipefail

# Get script and root directories
SCRIPT_DIR=$(cd `dirname $0` && pwd)
ROOT=$(cd "${SCRIPT_DIR}/../../" && pwd)

#DRIVER_CPU=${DRIVER_CPU:-8}
#DRIVER_MEM=${DRIVER_MEM:-26000M}
#DRIVER_MEMORY_OVERHEAD=${DRIVER_MEMORY_OVERHEAD:-6000M}
#EXECUTOR_CPU=${EXECUTOR_CPU:-8}
#EXECUTOR_MEM=${EXECUTOR_MEM:-26000M}
#EXECUTOR_MEMORY_OVERHEAD=${EXECUTOR_MEMORY_OVERHEAD:-6000M}
#INSTANCES=${INSTANCES:-4}
#SIZE=${SIZE:-1}

EXTRA_CLASSPATHS='/opt/spark/jars/*'
EXECUTOR_JAVA_OPTIONS="-Dsun.nio.PageAlignDirectMemory=true"
DRIVER_JAVA_OPTIONS="-Dsun.nio.PageAlignDirectMemory=true"

export SPARK_EXECUTOR_CORES=$EXECUTOR_CPU
export SPARK_DRIVER_MEMORY=$DRIVER_MEM
export SPARK_EXECUTOR_MEMORY=$EXECUTOR_MEM

SHUFFLE_DATA_BUCKET=zrlio-tmp/psp/s3-tpcds-shuffle/${PROCESS_TAG}

# Define Hadoop configurations
SPARK_HADOOP_S3A_CONFIG=(
    # Required
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    --conf spark.hadoop.fs.s3a.access.key=$S3A_ACCESS_KEY
    --conf spark.hadoop.fs.s3a.secret.key=$S3A_SECRET_KEY
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
    --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT
    --conf spark.hadoop.fs.s3a.path.style.access=true
    --conf spark.hadoop.fs.s3a.fast.upload=true

    --conf spark.driver.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.1.jar'
    --conf spark.executor.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.1.jar'
)

# Define Spark configurations 
export SPARK_S3_SHUFFLE_CONFIG=( 
    --conf spark.shuffle.star.rootDir="$SHUFFLE_DATA_PREFIX/tpcds/$PROCESS_TAG"
)

# Submit Spark job
${SPARK_HOME}/bin/spark-submit \
    --master k8s://$K8S_SERVER \
    --deploy-mode cluster \
    --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTIONS" \
    --conf "spark.executor.extraJavaOptions=$EXECUTOR_JAVA_OPTIONS" \
    --name ce-tpcds-$SIZE-$PROCESS_TAG-${INSTANCES}x${EXECUTOR_CPU}--$EXECUTOR_MEM \
    --conf spark.executor.instances=$INSTANCES \
    --conf spark.jars.ivy=/tmp/.ivy \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer=128mb \
    "${SPARK_HADOOP_S3A_CONFIG[@]}" \
    "${SPARK_S3_SHUFFLE_CONFIG[@]}" \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="$LOGS_DATA_PREFIX/tpcds" \
    --conf spark.kubernetes.container.image.pullSecrets=$PULL_SECRET_NAME \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=$K8S_NAMESPACE-manager \
    --conf spark.kubernetes.driver.podTemplateFile=$ROOT/templates/driver.yml \
    --conf spark.kubernetes.executor.podTemplateFile=$ROOT/templates/executor.yml \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.driver.extraClassPath=$EXTRA_CLASSPATHS \
    --conf spark.executor.extraClassPath=$EXTRA_CLASSPATHS \
    --conf spark.driver.memoryOverhead=$DRIVER_MEMORY_OVERHEAD \
    --conf spark.kubernetes.driver.request.cores=$DRIVER_CPU \
    --conf spark.kubernetes.driver.limit.cores=$DRIVER_CPU \
    --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
    --conf spark.kubernetes.executor.request.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.executor.limit.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.container.image=$IMAGE \
    --conf spark.kubernetes.namespace=$K8S_NAMESPACE \
	--class com.ibm.spark.perf.tpcds.runners.TPCDSBenchmarkCLI \
    local:///opt/spark/jars/sparksqlperformance_2.12-0.3.0-SNAPSHOT.jar \
        --create-tables true \
        --generate-table-stats true \
        --generate-column-stats true \
        --dataset-base-location s3a://tpc-ds/ \
        --result-location ${OUTPUT_DATA_PREFIX}/tpcds/${PROCESS_TAG} \
        --save-mode overwrite \
        --format parquet \
        --scale-factor $SIZE \
        --num-iterations $NUM_ITERATIONS \
        --log-level INFO

#mc rm -r --force ${S3A_REGION}/${SHUFFLE_DATA_BUCKET} || true
#mc rm -r --force ${S3A_REGION}/psp/output/tpcds/$PROCESS_TAG || true
