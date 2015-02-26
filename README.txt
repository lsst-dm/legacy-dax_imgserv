# Useful link:
http://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask

# To install flask:
sudo aptitude install python-flask

# To run some quick tests:

  # run the server
  python bin/imageServer.py

  # and fetch the urls:
  http://localhost:5000/image
  http://localhost:5000/image/v0/raw?ra=359.195&dec=-0.1055&filter=
  http://localhost:5000/image/v0/raw/cutout?ra=359.195&dec=-0.1055&filter=r&width=30.0&height=45.0
  http://localhost:5000/image/v0/raw/cutoutPixel?ra=359.195&dec=-0.1055&filter=r&width=30&height=45
  http://localhost:5000/image/v0/deepCoadd?ra=19.36995&dec=-0.3146&filter=r
  http://localhost:5000/image/v0/deepCoadd/cutout?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235
  http://localhost:5000/image/v0/deepCoadd/cutoutPixel?ra=19.36995&dec=-0.3146&filter=r&width=115&height=235

  # more coming soon...
