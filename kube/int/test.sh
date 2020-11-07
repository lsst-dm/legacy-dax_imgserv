#!/bin/bash -ex

echo "ImgServe smoke test at lsst-lsp-int ..."
curl --fail -o /tmp/imgserv_int_circle.fits -L \
"https://lsst-lsp-int.ncsa.illinois.edu/api/image/soda/sync?ID=default.calexp.r&POS=CIRCLE+320.806258+-0.3313935+0.01"
