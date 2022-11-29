#!/usr/bin/env bash
set -e

# IBM Cloud Container Registy namespace
NAMESPACE=amnestorov

ibmcloud login -r 'us-south' --apikey @~/.config/cloud.ibm.com.apikey.json
ibmcloud iam oauth-tokens | sed -ne '/IAM token/s/.* //p' | docker login -u iambearer --password-stdin us.icr.io/${NAMESPACE}
