#!/bin/bash
# script to be used when using the impresso-pycommons:v3 docker image, 
# which uses daskdev/dask:latest-py3.11 as base image.

echo $USER_NAME

# move to directory containing init script
cd /home/$USER_NAME/dhlab-data/data/$USER_NAME-data

# make init script exectuable and execute it.
chmod -x init_rebuilt_runai.sh 
. init_rebuilt_runai.sh

# sanity check
echo $log_file

# change back to /home/$USER_NAME
cd

# locally in a screen, the following should be run:
# kubectl port-forward {job-name}-0-0 8786:8786 &  kubectl port-forward {job-name}-0-0 8787:8787 

# launch screens
chmod -x impresso_pycommons/scripts/start_dask_cluster.sh 

screen -dmS scheduler sh impresso_pycommons/scripts/start_dask_cluster.sh scheduler
screen -dmS workers sh impresso_pycommons/scripts/start_dask_cluster.sh workers

echo "dask dashboard at localhost:8787/status"

screen -dmS rebuilt sh impresso_pycommons/scripts/start_dask_cluster.sh rebuilt