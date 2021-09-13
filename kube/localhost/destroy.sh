#!/bin/bash -x
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-stable-dax'}

kubectl delete ingress dax-imgserv-ingress --namespace $DAX_NAMESPACE
kubectl delete service dax-imgserv-service --namespace $DAX_NAMESPACE
kubectl delete deployment dax-imgserv-deployment --namespace $DAX_NAMESPACE
