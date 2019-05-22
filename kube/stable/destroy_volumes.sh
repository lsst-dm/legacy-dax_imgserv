#!/bin/bash -x
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-stable-dax'}

kubectl delete pvc dax-imgserv-datasets-claim --namespace $DAX_NAMESPACE
kubectl delete pv dax-stable-imgserv-datasets-volume
