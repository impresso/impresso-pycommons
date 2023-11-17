# https://stackoverflow.com/questions/11320782/bash-getting-pid-of-daemonized-screen-session

screen -dmS scheduler . /opt/conda/etc/profile.d/conda.sh ; dask scheduler --port 8786
screen -dmS workers . /opt/conda/etc/profile.d/conda.sh ; dask-worker localhost:8786 --workers 36 --nthreads 1 --memory-limit 6G
