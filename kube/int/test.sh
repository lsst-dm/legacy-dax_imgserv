#!/bin/bash -ex

echo "ImgServe smoke test at lsst-lsp-int ..."
curl --fail -o /tmp/lsp_int_test.fits -L \
"https://lsst-lsp-int.ncsa.illinois.edu/api/image/soda/sync?ID=default.calexp.r&POS=CIRCLE+216.68+-0.53+0.01"
