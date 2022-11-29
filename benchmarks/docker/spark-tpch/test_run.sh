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
        --conf spark.shuffle.star.rootDir="$SHUFFLE_DATA_PREFIX/tpch/$PROCESS_TAG"
)


# Submit Spark job
${SPARK_HOME}/bin/spark-submit \
    --master k8s://$K8S_SERVER \
    --deploy-mode cluster \
    --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTIONS" \
    --conf "spark.executor.extraJavaOptions=$EXECUTOR_JAVA_OPTIONS" \
    --name ce-tpch-$SIZE-$PROCESS_TAG-${INSTANCES}x${EXECUTOR_CPU}--$EXECUTOR_MEM \
    --conf spark.executor.instances=$INSTANCES \
    --conf spark.jars.ivy=/tmp/.ivy \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer=128mb \
    "${SPARK_HADOOP_S3A_CONFIG[@]}" \
    "${SPARK_S3_SHUFFLE_CONFIG[@]}" \
    --conf spark.eventLog.enabled=true \
    --conf spark.scheduler.scaleout.enabled=true \
    --conf spark.eventLog.dir="$LOGS_DATA_PREFIX/test-custom-spark-tpch-sql-$2-sf${SIZE}-ex${INSTANCES}" \
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
    --conf spark.locality.wait=0s \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.sql.partitions=500 \
    --conf spark.default.parallelism=500 \
    --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=10000s \
    --conf spark.kubernetes.executor.request.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.executor.limit.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.container.image=$IMAGE \
    --conf spark.kubernetes.namespace=$K8S_NAMESPACE \
    --conf spark.kubernetes.file.upload.path=s3a://$MINIO_OUTPUT_BUCKET/k8s \
    --class "main.scala.TpchQuery" \
    /home/nes/intelligent-scale-out/benchmarks/docker/spark-tpch/spark-tpc-h-queries_2.12-1.0.jar \
    "$@"
