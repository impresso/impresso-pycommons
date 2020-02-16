# https://stackoverflow.com/questions/11320782/bash-getting-pid-of-daemonized-screen-session

screen -dmS dask-sched-pycommons-test dask-scheduler
screen -dmS dask-work-pycommons-test dask-worker localhost:8786 --nprocs 8\
 --nthreads 1 --memory-limit 2G --local-directory=dask-worker-space
