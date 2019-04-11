curl -o ./output/post_calexp_i30.fits -X POST \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $LSP_TOKEN" \
-d @../input/test_calexp_i30.json http://$IS_EP/api/image/soda/sync