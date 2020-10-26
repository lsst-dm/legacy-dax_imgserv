curl -o /tmp/post_calexp_circle.fits -X POST \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $LSP_TOKEN" \
-d @../input/test_calexp_circle.json -L https://$IMGSERV_EP/api/image/soda/sync