#!/usr/bin/env bash

# IBM CCR region, namespace, repo, source image, and tag
REGION=us
CCR_NAMESPACE=amnestorov
REPO=${REGION}.icr.io/${CCR_NAMESPACE}
SOURCE_IMAGE=spark
TAG=custom-3.2.2

# k8s namespace
K8S_NAMESPACE=nes

# Secret name for CCR access
PULL_SECRET_NAME=amnestorov

# Submit spark image to k8s
${SPARK_HOME}/bin/spark-submit \
    --master k8s://https://9.4.244.122:6443 \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.container.image.pullSecrets=${PULL_SECRET_NAME} \
    --conf spark.kubernetes.container.image=${REPO}/${SOURCE_IMAGE}:${TAG} \
    --conf spark.kubernetes.namespace=${K8S_NAMESPACE} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=${K8S_NAMESPACE}-manager \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.driver.extraJavaOptions="-verbose:gc -XX:+UseG1GC" \
    --conf spark.hadoop.fs.s3a.access.key=${S3A_ACCESS_KEY} \
    --conf spark.hadoop.fs.s3a.secret.key=${S3A_SECRET_KEY} \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf spark.hadoop.fs.s3a.endpoint=${S3A_ENDPOINT} \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.kubernetes.file.upload.path=s3a://nes \
    ../spark/examples/jars/spark-examples_2.12-3.2.2.jar
