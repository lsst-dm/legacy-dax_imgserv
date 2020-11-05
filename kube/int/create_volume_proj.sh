#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-int-dax'}

kubectl create -f dax-imgserv-project-volume.yaml
kubectl create -f dax-imgserv-project-claim.yaml --namespace $DAX_NAMESPACE
