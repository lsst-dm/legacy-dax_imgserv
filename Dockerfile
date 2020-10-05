FROM lsstsqre/centos:w_latest

MAINTAINER Kenny Lo <kennylo@slac.stanford.edu>

# Add user for docker build/test/publish in jenkins
USER root
ARG USERNAME=jenkins
ARG UID=48435
ARG GID=202
RUN groupadd -g $GID -o $USERNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $USERNAME

# install redis
RUN yum -y install epel-release
RUN yum -y install redis

# Setup Dependencies
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
    conda install -y mysqlclient pymysql; \
    LDFLAGS=-fno-lto pip install uwsgi'

# switch to lsst user
USER lsst
WORKDIR /app

ADD requirements.txt .
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   pip install --no-cache-dir --user -r requirements.txt'

# Add the code in
ADD . /app
ADD /rootfs /

RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   setup lsst_distrib; \
   pip install --no-cache-dir --user .'

ENV UWSGI_THREADS=40
ENV UWSGI_PROCESSES=1
ENV UWSGI_OFFLOAD_THREADS=10
ENV UWSGI_WSGI_FILE=/app/bin/imageServer.py
ENV UWSGI_CALLABLE=app

# Start up the services
CMD ./bin/run_imgserv.sh
