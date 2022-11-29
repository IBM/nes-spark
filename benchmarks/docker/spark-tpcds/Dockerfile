ARG REPO
ARG IMAGE 
ARG TAG 

FROM ${REPO}/${IMAGE}:${TAG}

RUN echo "Building spark-tpcdc-benchmark.."
COPY docker/spark-tpcds/dsdgen/ "/opt/dsdgen/"
COPY docker/spark-tpcds/jars/*.jar "${SPARK_HOME}/jars/"

USER ${SPARK_USER}
