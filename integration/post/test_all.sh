#!/usr/bin/env bash
echo  "Enter ImgServ Endpoint (e.g. localhost:5000):"
read IS_EP
export IS_EP
echo "Enter token:"
read LSP_TOKEN
export LSP_TOKEN
# raw
./test_post_raw_i1.sh
./test_post_raw_i2.sh
./test_post_raw_i3.sh
./test_post_raw_i4.sh
./test_post_raw_i5.sh
./test_post_raw_i18.sh
./test_post_raw_i19.sh

# calexp
./test_post_calexp_i6.sh
./test_post_calexp_i7.sh
./test_post_calexp_i8.sh
./test_post_calexp_i9.sh
./test_post_calexp_i10.sh
./test_post_calexp_i11.sh
./test_post_calexp_i12.sh
./test_post_calexp_i30.sh
./test_post_calexp_i31.sh
./test_post_calexp_i32.sh
./test_post_calexp_i33.sh

# deepcoadd
./test_post_deepcoadd_i13.sh
./test_post_deepcoadd_i14.sh
./test_post_deepcoadd_i15.sh
./test_post_deepcoadd_i16.sh
./test_post_deepcoadd_i17.sh
./test_post_deepcoadd_i20.sh
./test_post_deepcoadd_i21.sh
./test_post_deepcoadd_i22.sh
