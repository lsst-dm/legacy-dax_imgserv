#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-localhost-dax'}

kubectl create -f namespace-localhost.json
kubectl create -f dax-imgserv-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-service.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-ingress.yaml --namespace $DAX_NAMESPACE
