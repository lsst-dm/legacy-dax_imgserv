#!/bin/bash -ex

echo "ImgServe smoke test at lsst-lsp-int ..."
curl --fail -o /tmp/imgserv_int_circle.fits -L \
"https://lsst-lsp-int.ncsa.illinois.edu/api/image/soda/sync?ID=sdss_stripe82_01.calexp.r&POS=CIRCLE+37.644598+0.104625+0.027777777"
