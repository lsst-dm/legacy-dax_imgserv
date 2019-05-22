#!/bin/bash -x
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-int-dax'}

kubectl delete pvc datasets-lsst-lsp-int-dax-claim --namespace $DAX_NAMESPACE
kubectl delete pv datasets-lsst-lsp-int-dax
