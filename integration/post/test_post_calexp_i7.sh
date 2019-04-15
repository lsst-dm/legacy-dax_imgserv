curl -o ./output/post_calexp_i7.fits -X POST \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $LSP_TOKEN" \
-d @../input/test_calexp_i7.json -L https://$IS_EP/api/image/v1/DC_W13_Stripe82