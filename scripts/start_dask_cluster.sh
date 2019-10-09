screen -dmS dask-sched-pycommons-test dask-scheduler
screen -dmS dask-work-pycommons-test dask-worker localhost:8786 --nprocs 8\
 --nthreads 1 --memory-limit 2G --local-directory=dask-worker-space
