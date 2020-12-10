FROM lsstsqre/centos:w_2020_46

MAINTAINER Kenny Lo <kennylo@slac.stanford.edu>

# Add user for docker build/test/publish in jenkins
USER root
ARG USERNAME=jenkins
ARG UID=48435
ARG GID=202
RUN groupadd -g $GID -o $USERNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $USERNAME

# install JRE for sodalint
RUN yum -y install java-11-openjdk

# install redis
RUN yum -y install epel-release
RUN yum -y install redis

# Setup Dependencies
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
    LDFLAGS=-fno-lto pip install uwsgi'

# switch to lsst user
USER lsst
WORKDIR /app

ADD requirements.txt .
RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   pip install --no-cache-dir --user -r requirements.txt'

# Add the code in
ADD . /app
# Add /etc
ADD /rootfs /

RUN /bin/bash -c 'source /opt/lsst/software/stack/loadLSST.bash; \
   setup lsst_distrib; \
   pip install --no-cache-dir --user .'

USER root
# remove unneeded stuff
RUN rm -rf /app/kube /app/integration /app/doc /app/Dockerfile

# run imgserv as lsst user
USER lsst

ENV UWSGI_THREADS=40
ENV UWSGI_PROCESSES=1
ENV UWSGI_OFFLOAD_THREADS=10
ENV UWSGI_WSGI_FILE=/app/bin/imageServer.py
ENV UWSGI_CALLABLE=app

# Start up the services
CMD ./bin/run_imgserv.sh
