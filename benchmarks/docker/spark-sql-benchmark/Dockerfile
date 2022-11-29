ARG REPO
ARG IMAGE
ARG TAG

FROM ${REPO}/${IMAGE}:${TAG}

RUN echo "Building spark-sql-benchmark.."
COPY docker/spark-sql-benchmark/jars/*.jar "${SPARK_HOME}/jars/"

USER ${SPARK_USER}
