#!/bin/bash -ex

echo "ImgServe smoke test at lsst-lsp-stable ..."
curl -o /tmp/lsp_stable_test.fits -L \
"https://lsst-lsp-stable.ncsa.illinois.edu/api/image/soda/sync?ID=default.calexp.r&POS=CIRCLE+216.68+-0.53+0.01"
