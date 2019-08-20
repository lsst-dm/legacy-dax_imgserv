#!/bin/bash -ex

echo "ImgServ smoke test at lsst-lsp-stable ..."
curl --fail -o /tmp/imgserv_stable_circle.fits "https://lsst-lsp-stable.ncsa.illinois.edu/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=CIRCLE+37.644598+0.104625+0.027777777"
