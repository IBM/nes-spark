# Minio server, access key, and secret key
SERVER=
ACCESS_KEY=
SECRET_KEY=

# Add environment variables
echo "# S3 service environment variables" >> ~/.bashrc
echo "export S3A_ENDPOINT='http://${SERVER}'" >> ~/.bashrc
echo "export S3A_BUCKET_PREFIX='s3a://${ACCESS_KEY}'" >> ~/.bashrc
echo "export S3A_ACCESS_KEY='${ACCESS_KEY}'" >> ~/.bashrc
echo "export S3A_SECRET_KEY='${SECRET_KEY}'" >> ~/.bashrc
