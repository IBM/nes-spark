# Setup K8s environment variables
export K8S_SERVER="https://$(cat params.json | jq -r '.k8s_server')"
export K8S_NAMESPACE="$(cat params.json | jq -r '.k8s_namespace')"

# Setup CCR secret name
export PULL_SECRET_NAME="$(cat params.json | jq -r '.pull_secret_name')"

# Setup minio server environmnet variables
export S3A_REGION="$(cat params.json | jq -r '.s3a_region')"
if [[ -z "${S3A_ENDPOINT}" ]]; then
    export S3A_ENDPOINT="http:///$(cat params.json | jq -r '.minio_server')"
    export S3A_ACCESS_KEY="$(cat params.json | jq -r '.minio_access_key')"
    export S3A_SECRET_KEY="$(cat params.json | jq -r '.minio_secret_key')"
fi

# Setup benchmark runs input and output buckets environment variables
export INPUT_DATA_PREFIX="s3a://$(cat params.json | jq -r '.in_data_dir')"
export SHUFFLE_DATA_PREFIX="s3a://$(cat params.json | jq -r '.shuffle_data_dir')"
export LOGS_DATA_PREFIX="s3a://$(cat params.json | jq -r '.log_data_dir')"
export OUTPUT_DATA_PREFIX="s3a://$(cat params.json | jq -r '.out_data_dir')"
export MINIO_LOGS_BUCKET="$(cat params.json | jq -r '.log_data_dir')"
export MINIO_DLOGS_BUCKET="$(cat params.json | jq -r '.log_d_data_dir')"
export MINIO_OUTPUT_BUCKET="$(cat params.json | jq -r '.out_data_dir')"
