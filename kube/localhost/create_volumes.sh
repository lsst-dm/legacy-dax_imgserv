#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-localhost-dax'}

kubectl create -f dax-imgserv-datasets-volume.yaml
kubectl create -f dax-imgserv-datasets-claim.yaml --namespace $DAX_NAMESPACE
kubectl create -f dax-imgserv-project-volume.yaml
kubectl create -f dax-imgserv-project-claim.yaml --namespace $DAX_NAMESPACE
