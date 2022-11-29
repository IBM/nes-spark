SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

if [[ ! -d "${SPARK_HOME}" ]]; then
    echo "SPARK_HOME is not defined or does not exist."
    exit 1
fi
if [[ -z "${NAMESPACE}" ]]; then
    echo "NAMESPACE is not defined."
    exit 1
fi
if [[ -z "${SPARK_CONFIGMAP}" ]]; then
    echo "SPARK_CONFIGMAP is not defined."
    exit 1
fi

kubectl delete configmap ${SPARK_CONFIGMAP} 
kubectl create configmap ${SPARK_CONFIGMAP} -n $NAMESPACE \
    --from-file=core-site=${SCRIPT_DIR}/core-site.xml

        # --from-file=spark-defaults=${SPARK_HOME}/conf/spark-defaults.conf \
    # --from-file=log4j=${SPARK_HOME}/config/spark/log4j.properties \