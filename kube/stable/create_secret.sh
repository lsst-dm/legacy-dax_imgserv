#!/bin/bash -e
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-stable-dax'}

kubectl create secret generic dax-imgserv-config --from-file=./webserv.ini \
--namespace $DAX_NAMESPACE
kubectl create secret generic dax-db-auth --from-file=./webserv.ini \
--namespace $DAX_NAMESPACE