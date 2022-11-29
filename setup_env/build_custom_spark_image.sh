#!/usr/bin/env bash
set -euo pipefail

helpFunction()
{
   echo ""
   echo "Usage: ./build_image.sh -t tag"
   echo -e "\t-t Docker image tag name"
   exit 1 
}

# Get tag parameter
while getopts "t:b" opt
do
   case "$opt" in
      t ) TAG="$OPTARG" ;;
      b ) BUILD=True;;
      ? ) helpFunction ;;
   esac
done

# If the tag paramenter is unset use the default 'latest' tag
if [ "${TAG=unset}" = unset ];
then
   TAG=latest
fi

if [ "${BUILD=unset}" = unset ];
then
    BUILD=False
fi

FEATURES=(
    -Pkubernetes 
    -Phive
    -Phive-thriftserver
    -Pyarn
    -DskipTests
)

# Spark, Hadoop, and S3 libraries versions
SPARK_VERSION=3.3.0
# HADOOP_AWS_VERSION=3.2.0
HADOOP_AWS_VERSION=3.3.1
# AWS_JAVA_VERSION=1.11.375
AWS_JAVA_VERSION=1.11.900

# IBM CCR namespace, repo, and source image name
NAMESPACE=amnestorov
REPO=us.icr.io/${NAMESPACE}
SOURCE_IMAGE=spark

# Setup Spark distribution 
if [ ! -d "$SPARK_HOME"/jars ]; then
    echo "Downloading Spark S3 jars.."
	# Download Spark AWS S3 libraries (necessary for Minio)
    mkdir -p ${SPARK_HOME}/jars
	wget -P ${SPARK_HOME}/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar
	wget -P ${SPARK_HOME}/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_VERSION}.jar
	# Add SPARK_HOME environment variable
	GIT_HOME=$(cd .. && echo $PWD)
	echo '# Spark home directory' >> ~/.bashrc 
	echo "export SPARK_HOME='${GIT_HOME}/spark'" >> ~/.bashrc 
fi

if [ $BUILD == True ]; then
    echo "Building Spark source code.."
    # Build Spark
    (cd $SPARK_HOME && ./build/mvn "${FEATURES[@]}" clean package) 
    echo "Copying S3 set of jars.."
    cp -r $SPARK_HOME/jars/* $SPARK_HOME/assembly/target/scala-2.12/jars/
else
    echo "Copying potentially modifyed jars (core and/or sql).."
    cp $SPARK_HOME/sql/core/target/spark-sql_2.12-${SPARK_VERSION}.jar $SPARK_HOME/assembly/target/scala-2.12/jars/
    cp $SPARK_HOME/core/target/spark-core_2.12-${SPARK_VERSION}.jar $SPARK_HOME/assembly/target/scala-2.12/jars/
fi

# Build and push docker image 
echo "Building and pushing docker image.."
# Build a Java-based Docker image and push it to CCR 
(cd $SPARK_HOME && ./bin/docker-image-tool.sh -n -r $REPO -t $TAG build)
docker push ${REPO}/${SOURCE_IMAGE}:${TAG}
