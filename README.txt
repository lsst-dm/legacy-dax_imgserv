# Useful link:
http://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask

# To install flask:
sudo aptitude install python-flask

# To run some quick tests:

  # run the server
  python bin/imageServer.py

  # and fetch the urls:
  http://localhost:5000/api/image
  http://localhost:5000//api/image/v1/DC_W13_Strip82?ds=raw&ra=359.195&dec=-0.1055&filter=r
  http://localhost:5000/api/image/v1/DC_W13_Strip82?ds=raw&ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0&unit=arcsec
  http://localhost:5000/api/image/v1/DC_W13_Strip82?ds=raw&ra=359.195&dec=-0.1055&filter=r&width=30&height=45&unit=pixel
  http://localhost:5000/api/image/v1/DC_W13_Strip82?ds=deepcoadd&ra=19.36995&dec=-0.3146&filter=r
  http://localhost:5000/api/image/v1/DC_W13_Strip82?ds=deepcoadd&ra=19.36995&dec=-0.3146&filter=r&width=115&height=235&unit=arcsec
  http://localhost:5000/api/image/v1/DC_W13_Strip82?ds=deepcoadd&ra=19.36995&dec=-0.3146&filter=r&width=115&height=235&unit=pixel

  # more coming soon...
