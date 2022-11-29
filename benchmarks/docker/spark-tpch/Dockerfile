ARG REPO
ARG IMAGE
ARG TAG

FROM ${REPO}/${IMAGE}:${TAG}

RUN echo "Building spark-tpch.."
COPY docker/spark-tpch/jars/*.jar "${SPARK_HOME}/jars/"

USER ${SPARK_USER}
