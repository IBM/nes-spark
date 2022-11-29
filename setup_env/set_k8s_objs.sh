# K8s namespace
K8S_NAMESPACE=nes

# Create K8s service account, role, and rolebinding yaml descriptions
cp templ_k8s_objs.yaml my_k8s_objs.yaml
sed -i "s/TMP_NAMESPACE/${K8S_NAMESPACE}/g" my_k8s_objs.yaml

# Create service account, role, and rolebinding for the given namespace
kubectl apply -f my_k8s_objs.yaml

# Delete specific yaml
#rm my_k8s_objs.yaml

# Verify if the service account has permission to create/delete pods
echo "Checking if the new service account has create/delete permissions on pods..."
kubectl auth can-i create pod --as system:serviceaccount:${K8S_NAMESPACE}:${K8S_NAMESPACE}-manager -n $K8S_NAMESPACE
