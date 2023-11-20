#!/bin/bash
# https://stackoverflow.com/questions/11320782/bash-getting-pid-of-daemonized-screen-session
# goal is to launch detached screen sessions with the correct environment activated.
# these commands will be executed in a detached screen session and will instantiate
# the dask scheduler or workers based on the provided argument.
# replaces the following commands:
#screen -dmS scheduler dask scheduler --port 8786
#screen -dmS workers dask-worker localhost:8786 --nworkers 36 --nthreads 1 --memory-limit 6G

# Check if an argument is provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 <argument> \n"
  echo "Arguments (mutually exclusive):"
  echo "  scheduler - Launches a dask scheduler on port 8786."
  echo "  workers - Launches a dask scheduler on localhost:8786 with 36 workers (1 thread each) and 6G of memory-limit."
  echo "  rebuilt - Launches the rebuilder script given the parameters set as environment variables."
  #exit 1
fi

# Store the first argument in a variable
ARG=$1

# activate conda and print env used.
. /opt/conda/etc/profile.d/conda.sh 
conda activate rebuilt 
conda env list 

# Check the value of the argument and execute corresponding commands
case $ARG in
  "scheduler")
    echo "Launching the scheduler"
    dask scheduler --port 8786
    ;;
  "workers")
    echo "Launching the workers"
    dask worker localhost:8786 --nworkers 64 --nthreads 1 --memory-limit 6G
    ;;
  "rebuilt")
    echo "Launching the rebuilder script"
    python $pvc_path/impresso-pycommons/impresso_commons/text/rebuilder.py rebuild_articles --input-bucket=$input_bucket --log-file=$log_file --output-dir=$output_dir --output-bucket=$output_bucket --format=$format --filter-config=$filter_config --scheduler=localhost:8786
    ;;
  *)
    echo "Unknown option: $ARG"
    #exit 1
    ;;
esac
