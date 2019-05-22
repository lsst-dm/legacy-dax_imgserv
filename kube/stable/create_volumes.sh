#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-stable-dax'}

kubectl create -f dax-imgserv-datasets-volume.yaml
kubectl create -f dax-imgserv-datasets-claim.yaml --namespace $DAX_NAMESPACE
