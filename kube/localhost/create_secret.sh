#!/bin/bash -e
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-localhost-dax'}

kubectl create secret generic dax-imgserv-config --from-file=/Users/krughoff/projects/minikube/webserv.ini \
--namespace $DAX_NAMESPACE
kubectl create secret generic dax-db-auth --from-file=/Users/krughoff/projects/minikube/db-auth.yaml \
--namespace $DAX_NAMESPACE