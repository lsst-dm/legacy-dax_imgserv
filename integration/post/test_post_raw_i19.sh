curl -o ./output/post_raw_i19.fits -X POST \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $LSP_TOKEN" \
-d @../input/test_raw_i19.json http://$IS_EP/api/image/v1/DC_W13_Stripe82
