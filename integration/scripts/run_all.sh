#!/bin/bash

echo  "Enter the ImgServ Endpoint (e.g. http://localhost:5000):"
read IMGSERV_EP
export IMGSERV_EP

# echo "Enter token:"
# read LSP_TOKEN
# export LSP_TOKEN

for f in *.sh; do
  if [[ "$f" != "run_all.sh" ]]; then
    if ! bash "$f" -H; then break; fi
  fi
done
