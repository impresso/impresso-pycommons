#!/bin/bash
# Script to be used to launch the dask local cluster and rebuilder script. 
# If using Runai, more information available https://github.com/impresso/impresso-infrastructure/blob/main/howtos/runai.md.
# Will use the environment variables in pvc (on mnt point of cdhvm0002)
# "/home/$USER_NAME/dhlab-data/data/$USER_NAME-data/init_rebuilt_runai.sh" for the various options necessary.

# Default number of workers
DEFAULT_WORKERS=64

# Display script usage information
usage() {
  echo "Usage: $0 [-h|--help] [-w|--nworkers <num>]"
  echo "Options:"
  echo "  -h, --help       Display this help message"
  echo "  -w, --nworkers    Number of workers to use (default: $DEFAULT_WORKERS)"
  exit 1
}

ARG=$1

# Parse command-line options
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      usage
      ;;
    -w|--workers)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Error: Missing value for option -w|--workers"
        usage
      fi
      WORKERS=$1
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
  shift
done

# If number of workers is not provided, use the default value
WORKERS=${WORKERS:-$DEFAULT_WORKERS}

echo "Using user: $USER_NAME"

# move to directory containing init script
cd /home/$USER_NAME/dhlab-data/data/$USER_NAME-data

# make config script exectuable and execute it.
chmod -x config_rebuilt_runai.sh 
. config_rebuilt_runai.sh

# sanity check
echo "Sanity check: env. variable log_file: $log_file"

# change back to /home/$USER_NAME
cd

# locally in a screen, the following should be run:
# kubectl port-forward {job-name}-0-0 8786:8786 &  kubectl port-forward {job-name}-0-0 8787:8787 

# launch screens
echo "Launching the scheduler, workers and rebuilder script, with $WORKERS workers."
screen -dmS scheduler dask scheduler --port 8786
screen -dmS workers dask worker localhost:8786 --nworkers $WORKERS --nthreads 1 --memory-limit 6G

echo "dask dashboard at localhost:8787/status"

screen -dmS rebuilt python $pvc_path/impresso-pycommons/impresso_commons/text/rebuilder.py rebuild_articles --input-bucket=$input_bucket --log-file=$log_file --output-dir=$output_dir --output-bucket=$output_bucket --format=$format --filter-config=$filter_config --scheduler=localhost:8786