#!/bin/bash -ex

echo "ImgServe test at lsst-lsp-int ..."

curl --fail -o /tmp/imgserv_int_circle.fits "https://lsst-lsp-int.ncsa.illinois.edu/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=CIRCLE+37.644598+0.104625+100"
