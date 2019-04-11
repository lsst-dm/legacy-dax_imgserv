curl -o ./output/post_calexp_i31.fits -X POST \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $LSP_TOKEN" \
-d @../input/test_calexp_i31.json http://$IS_EP/api/image/soda/sync