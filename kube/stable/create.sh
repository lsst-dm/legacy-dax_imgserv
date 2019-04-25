#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-stable'}

# kubectl create -f dax-imgserv-datasets-volume.yaml
# kubectl create -f dax-imgserv-datasets-claim.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-service.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-ingress.yaml --namespace $DAX_NAMESPACE
