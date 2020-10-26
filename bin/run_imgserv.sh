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

# Start celery worker in imgserv container
# celery will start 1 worker process per CPU core, by default
celery -A lsst.dax.imgserv.jobqueue.imageworker worker --detach \
-Q celery,imageworker_queue \
--loglevel=INFO \
--logfile /tmp/imageworker_jobqueue.log

# start imgserv
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib
uwsgi --ini /etc/uwsgi/uwsgi.ini
