# Useful link:
http://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask

# To install flask:
sudo aptitude install python-flask

# To run some quick tests:

  # run the server
  python bin/imageServer.py

  # and fetch the urls:
  http://localhost:5000/api/image/soda/availability
  http://localhost:5000/api/image/soda/capabilities
  http://localhost:5000/api/image/soda/examples
  http://localhost:5000/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=CIRCLE+37.644598+0.104625+100
  http://localhost:5000/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=RANGE+37.616820222+37.67235778+0.07684722222+0.132402777
  http://localhost:5000/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=POLYGON+37.6580803+0.0897081+37.6580803+0.1217858+37.6186104+0.1006648
  http://localhost:5000/api/image/soda/sync?ID=DC_W13_Stripe82.calexp.r&POS=BRECT+37.644598+0.104625+100+100+pixel