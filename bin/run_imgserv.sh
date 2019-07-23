#!/bin/bash
# This script is to be run inside imgserv container ONLY
#
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib;
# Set home dir
cd /home/lsst
# Start redis server
redis-server /app/rootfs/etc/redis/redis.conf
# create the directory for results in /tmp
mkdir /tmp/imageworker_results
# Start celery worker in imgserv
# Note: by default, celery will start 1 worker process per CPU core
celery --detach -A lsst.dax.imgserv.jobqueue.imageworker worker \
--loglevel=INFO --logfile /tmp/img_jobqueue.log
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib
uwsgi --ini /etc/uwsgi/uwsgi.ini
