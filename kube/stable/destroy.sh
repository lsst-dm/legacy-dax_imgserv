#!/bin/bash -x
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-stable'}

kubectl delete ingress dax-imgserv-ingress --namespace $DAX_NAMESPACE
kubectl delete service dax-imgserv-service --namespace $DAX_NAMESPACE
kubectl delete deployment dax-imgserv-deployment --namespace $DAX_NAMESPACE
kubectl delete pvc dax-imgserv-datasets-claim --namespace $DAX_NAMESPACE
kubectl delete pv dax-stable-imgserv-datasets-volume
