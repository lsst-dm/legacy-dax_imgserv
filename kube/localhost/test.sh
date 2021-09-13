#!/bin/bash -ex

echo "ImgServe smoke test at lsst-lsp-localhost ..."
curl -o /tmp/lsp_localhost_test.fits -L \
"https://lsst-lsp-localhost.ncsa.illinois.edu/api/image/soda/sync?ID=default.calexp.r&POS=CIRCLE+216.68+-0.53+0.01"
