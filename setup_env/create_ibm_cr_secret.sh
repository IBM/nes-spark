PASSWORD=
EMAIL=
NAME=amnestorov
SERVER=us.icr.io
K8S_NAMESPACE=nes

kubectl create secret docker-registry $NAME --docker-server=$SERVER --docker-username=iamapikey --docker-password=$PASSWORD  --docker-email=$EMAIL
