FROM lsstsqre/centos:w_latest

MAINTAINER Kenny Lo <kennylo@slac.stanford.edu>

WORKDIR /app

# Setup Dependencies
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
    conda install -y mysqlclient pymysql; \
    LDFLAGS=-fno-lto pip install uwsgi'

ADD requirements.txt .
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   pip install --no-cache-dir --user -r requirements.txt'

# Add the code in
ADD . /app
ADD /rootfs /

RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   pip install --no-cache-dir --user .'

ENV UWSGI_THREADS=40
ENV UWSGI_PROCESSES=1
ENV UWSGI_OFFLOAD_THREADS=10
ENV UWSGI_WSGI_FILE=/app/bin/imageServer.py
ENV UWSGI_CALLABLE=app

# Activate conda, setup lsst_distrib
CMD ["bash", "-c", \
  "source /opt/lsst/software/stack/loadLSST.bash; \
     setup lsst_distrib;  \
     export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib; \
     uwsgi --ini /etc/uwsgi/uwsgi.ini; \
"]
