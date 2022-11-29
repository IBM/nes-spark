#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd `dirname $0` && pwd)
ROOT=$(cd "${SCRIPT_DIR}/../../" && pwd)
echo $ROOT

export KUBECONFIG=${KUBECONFIG:-"~/.kube/lab-config"}
export KUBERNETES_SERVER=""
export NAMESPACE=${NAMESPACE:-""}
export KUBERNETES_SERVICE_ACCOUNT=${KUBERNETES_SERVICE_ACCOUNT:-"${NAMESPACE}-manager"}
export PULL_SECRETS_NAME=${PULL_SECRETS_NAME:-"zrlio"}
export SPARK_CONFIGMAP=${SPARK_CONFIGMAP:-"spark-config"}

export SPARK_HOME=""
export DOCKER_REPO=""
export VERSION="3.2.1"
export DOCKER="podman"
export PREFIX="workload-"

S3A_ENDPOINT=""
S3A_BUCKET_PREFIX="s3a://tpc-ds"
S3A_ACCESS_KEY=""
S3A_SECRET_KEY=""

# https://docs.min.io/docs/disaggregated-spark-and-hadoop-hive-with-minio.html
export SPARK_HADOOP_S3A_CONFIG=(
    --conf spark.hadoop.fs.s3a.access.key=${S3A_ACCESS_KEY}
    --conf spark.hadoop.fs.s3a.secret.key=${S3A_SECRET_KEY}
    --conf spark.hadoop.fs.s3a.path.style.access=true
    --conf spark.hadoop.fs.s3a.block.size=512M
    # --conf spark.hadoop.fs.s3a.buffer.dir=/tmp/s3a
    --conf spark.hadoop.fs.s3a.committer.magic.enabled=false
    --conf spark.hadoop.fs.s3a.committer.name=directory
    --conf spark.hadoop.fs.s3a.committer.staging.abort.pending.uploads=true
    --conf spark.hadoop.fs.s3a.committer.staging.conflict-mode=append
    --conf spark.hadoop.fs.s3a.committer.staging.tmp.path=/tmp/staging
    --conf spark.hadoop.fs.s3a.committer.staging.unique-filenames=true
    --conf spark.hadoop.fs.s3a.committer.threads=2048
    --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000
    --conf spark.hadoop.fs.s3a.connection.maximum=8192 # maximum number of concurrent conns
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
    --conf spark.hadoop.fs.s3a.connection.timeout=200000
    --conf spark.hadoop.fs.s3a.endpoint=${S3A_ENDPOINT}
    --conf spark.hadoop.fs.s3a.fast.upload.active.blocks=2048 # number of parallel uploads
    --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk # use disk as the buffer for uploads
    --conf spark.hadoop.fs.s3a.fast.upload=true # turn on fast upload mode
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    --conf spark.hadoop.fs.s3a.max.total.tasks=2048 # maximum number of parallel tasks
    --conf spark.hadoop.fs.s3a.multipart.size=512M # size of each multipart chunk
    --conf spark.hadoop.fs.s3a.multipart.threshold=512M # size before using multipart uploads
    --conf spark.hadoop.fs.s3a.socket.recv.buffer=65536 # read socket buffer hint
    --conf spark.hadoop.fs.s3a.socket.send.buffer=65536 # write socket buffer hint
    --conf spark.hadoop.fs.s3a.threads.max=2048 # maximum number of threads for S3A
    --conf spark.driver.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.1.jar'
    --conf spark.executor.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.1.jar'
)

IMAGE=${IMAGE:-"us.icr.io/zrlio/workload-spark-tpcds:3.2.1"}
DRIVER_CPU=${DRIVER_CPU:-8}
DRIVER_MEM=${DRIVER_MEM:-26000M}
DRIVER_MEMORY_OVERHEAD=${DRIVER_MEMORY_OVERHEAD:-6000M}
EXECUTOR_CPU=${EXECUTOR_CPU:-8}
EXECUTOR_MEM=${EXECUTOR_MEM:-26000M}
EXECUTOR_MEMORY_OVERHEAD=${EXECUTOR_MEMORY_OVERHEAD:-6000M}
INSTANCES=${INSTANCES:-4}
SIZE=${SIZE:-100}

TPCDS_COS_PUBLIC_URL="${S3A_ENDPOINT}"
TPCDS_COS_PRIVATE_URL="${S3A_ENDPOINT}"
TPCDS_COS_ACCESS_KEY="${S3A_ACCESS_KEY}"
TPCDS_COS_SECRET_KEY="${S3A_SECRET_KEY}"
TPCDS_BASE_LOCATION="${S3A_BUCKET_PREFIX}"
TPCDS_OUTPUT_LOCATION="${S3A_BUCKET_PREFIX}/output"
TPCDS_DATAGEN_LOCATION="/opt/"
# TPCDS_SCALE_FACTORS="10,100"
TPCDS_SCALE_FACTORS="1000"
LANG="en_US"

export SPARK_EXECUTOR_CORES=$EXECUTOR_CPU
export SPARK_DRIVER_MEMORY=$DRIVER_MEM
export SPARK_EXECUTOR_MEMORY=$EXECUTOR_MEM

${SPARK_HOME}/bin/spark-submit \
    --master k8s://$KUBERNETES_SERVER \
    --deploy-mode cluster \
    --name tpcds-datagen-${SIZE} \
    --conf spark.executor.instances=$INSTANCES \
    --conf spark.kubernetes.driverEnv.LANG="${LANG}" \
    --conf spark.kubernetes.driverEnv.TPCDS_COS_PUBLIC_URL="${TPCDS_COS_PUBLIC_URL}" \
    --conf spark.kubernetes.driverEnv.TPCDS_COS_PRIVATE_URL="${TPCDS_COS_PRIVATE_URL}" \
    --conf spark.kubernetes.driverEnv.TPCDS_COS_ACCESS_KEY="${TPCDS_COS_ACCESS_KEY}" \
    --conf spark.kubernetes.driverEnv.TPCDS_COS_SECRET_KEY="${TPCDS_COS_SECRET_KEY}" \
    --conf spark.kubernetes.driverEnv.TPCDS_BASE_LOCATION="${TPCDS_BASE_LOCATION}" \
    --conf spark.kubernetes.driverEnv.TPCDS_OUTPUT_LOCATION="${TPCDS_OUTPUT_LOCATION}" \
    --conf spark.kubernetes.driverEnv.TPCDS_DATAGEN_LOCATION="${TPCDS_DATAGEN_LOCATION}" \
    --conf spark.kubernetes.driverEnv.TPCDS_SCALE_FACTORS="${TPCDS_SCALE_FACTORS}" \
    --conf spark.executorEnv.LANG="${LANG}" \
    --conf spark.executorEnv.TPCDS_COS_PUBLIC_URL="${TPCDS_COS_PUBLIC_URL}" \
    --conf spark.executorEnv.TPCDS_COS_PRIVATE_URL="${TPCDS_COS_PRIVATE_URL}" \
    --conf spark.executorEnv.TPCDS_COS_ACCESS_KEY="${TPCDS_COS_ACCESS_KEY}" \
    --conf spark.executorEnv.TPCDS_COS_SECRET_KEY="${TPCDS_COS_SECRET_KEY}" \
    --conf spark.executorEnv.TPCDS_BASE_LOCATION="${TPCDS_BASE_LOCATION}" \
    --conf spark.executorEnv.TPCDS_OUTPUT_LOCATION="${TPCDS_OUTPUT_LOCATION}" \
    --conf spark.executorEnv.TPCDS_DATAGEN_LOCATION="${TPCDS_DATAGEN_LOCATION}" \
    --conf spark.executorEnv.TPCDS_SCALE_FACTORS="${TPCDS_SCALE_FACTORS}" \
    --conf spark.jars.ivy=/tmp/.ivy \
    "${SPARK_HADOOP_S3A_CONFIG[@]}" \
    --conf spark.network.timeout=10000 \
    --conf spark.executor.heartbeatInterval=20000 \
    --conf spark.kubernetes.appKillPodDeletionGracePeriod=5 \
    --conf spark.kubernetes.container.image.pullSecrets=${PULL_SECRETS_NAME} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=${KUBERNETES_SERVICE_ACCOUNT} \
    --conf spark.kubernetes.driver.podTemplateFile=${ROOT}/templates/driver.yml \
    --conf spark.kubernetes.executor.podTemplateFile=${ROOT}/templates/executor.yml \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.driver.limit.cores=$DRIVER_CPU \
    --conf spark.driver.cores=$DRIVER_CPU \
    --conf spark.driver.memoryOverhead=$DRIVER_MEMORY_OVERHEAD \
    --conf spark.kubernetes.driver.request.cores=$DRIVER_CPU \
    --conf spark.kubernetes.driver.limit.cores=$DRIVER_CPU \
    --conf spark.kubernetes.executor.limit.cores=$EXECUTOR_CPU \
    --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
    --conf spark.executor.request.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.container.image=$IMAGE \
    --conf spark.kubernetes.namespace=$NAMESPACE \
    --class com.ibm.spark.perf.tpcds.runners.GenerateTPCDSData \
    local:///opt/spark/jars/sparksqlperformance_2.12-0.3.0-SNAPSHOT.jar
