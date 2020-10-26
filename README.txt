# Useful link:
http://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask

# Get code from GitHub
git clone https://github.com/lsst/dax_imgserv.git
cd dax_imgserv

# Install the requirements
pip install -r requirements.txt

# To run some quick tests:

  # run the server
  python bin/imageServer.py

  # and fetch the urls:
  http://127.0.0.1:5000/api/image/soda/availability
  http://127.0.0.1:5000/api/image/soda/capabilities
  http://127.0.0.1:5000/api/image/soda/examples
  http://127.0.0.1:5000/api/image/soda/sync?ID=ci_hsc_gen3.calexp.r&POS=CIRCLE+320.94+-0.289128+0.01
  http://127.0.0.1:5000/api/image/soda/sync?ID=ci_hsc_gen3.calexp.r&POS=RANGE+320.94+321.04+-0.289128+-0.279128
  http://127.0.0.1:5000/api/image/soda/sync?ID=ci_hsc_gen3.calexp.r&POS=POLYGON+320.94+-0.289128+320.95+-0.279128+320.96+-0.289128
  http://127.0.0.1:5000/api/image/soda/sync?ID=ci_hsc_gen3.calexp.r&POS=BBOX+320.94+-0.289128+0.01+0.01