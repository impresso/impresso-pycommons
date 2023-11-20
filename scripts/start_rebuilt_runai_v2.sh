#!/bin/bash
# script to be used when using the impresso-pycommons:v2 docker image, 
# which uses nvidia/cuda:11.0.3-cudnn8-devel-ubuntu20.04 as base image.

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
screen -dmS scheduler dask scheduler --port 8786
screen -dmS workers dask worker localhost:8786 --nworkers 50 --nthreads 1 --memory-limit 6G

echo "dask dashboard at localhost:8787/status"

screen -dmS rebuilt python $pvc_path/impresso-pycommons/impresso_commons/text/rebuilder.py rebuild_articles --input-bucket=$input_bucket --log-file=$log_file --output-dir=$output_dir --output-bucket=$output_bucket --format=$format --filter-config=$filter_config --scheduler=localhost:8786