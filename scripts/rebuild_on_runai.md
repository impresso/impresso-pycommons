## Steps and How-to run _impresso_'s rebuilder on Runai.

Documentation to setup Runai and run the `rebuilder.py` script on the iccluster using it.
Last modification: Nov. 2023.

### Step 1: Setup Runai

Before anything else, Runai should be setup and configured. This can be done by following the tutorial(s) on the following sources:
- [RunAi Starter gDoc](https://docs.google.com/document/d/1--QB_9PLSK6LAEDfIirR5aptWHi9Ri5BxCwT7LXz29M/edit)
- [RunAi Crash Course gSlides](https://docs.google.com/presentation/d/15UY_8wZGGQW_sLzcaOPaMjeU5nneraRfsWPjf8uF7cc/edit#slide=id.p)
- [IC-IT RunAi Documentation](https://icitdocs.epfl.ch/display/clusterdocs/Getting+Started+with+RunAI)

To verify the setup was correctly done, run `runai list` on the terminal (while on EPFL intranet, use VPN if not on campus). The output should be similar to: 
```
Showing jobs for project dhlab-{gaspar_username}
NAME  STATUS  AGE  NODE  IMAGE  TYPE  PROJECT  USER  GPUs Allocated (Requested)  PODs Running (Pending)  SERVICE URL(S)
```

Once the setup is done, no need to repeat this step on future run.

### Step 2: Code version and IC Registry

Ensure the correct Docker image is currently in the [EPFL IC registry](https://ic-registry.epfl.ch/harbor/projects).
If you have done significant code changes since, consider updating the [dhlab/impresso_pycommons](https://ic-registry.epfl.ch/harbor/projects/25/repositories/dhlab%2Fimpresso_pycommons) Docker image there.
If you wish to modify the version tag of the image uploaded, do so in the bash script `/impresso_commons/scripts/docker_image.sh`.

Once the `Dockerfile` and `docker_image.sh` match your needs, run:
```
. scripts/docker_image.sh         
```

Current image version to use: [v3](https://ic-registry.epfl.ch/harbor/projects/25/repositories/dhlab%2Fimpresso_pycommons/tags/v3)

### Step 3: Prep the PVC

PVC is persistent storage available on RunAi pods. Therefore any code, configurations, logs etc you might want to access for each job should be put there. 

**Create user folder on `cdhvm0002`** (only once):
First, if it doesn't already exist, create the folder for you user on `cdhvm0002.xaas.epfl.ch`:
```
/mnt/u12632_cdh_dhlab_002_files_nfs/data/{gaspar_username}-data        
```

Any files put in this folder will be accessible within the pod in folder:
```
/home/{gaspar_username}/dhlab-data/data/{gaspar_username}-data/
```

**Create or modify the desired config file**

A script named `config_rebuilt_runai.sh` instantiating all necessary options and environment variables for the `text/rebuilder.py` script is expected to be on the PCV.
An example for this script is `example_config_rebuilt_runai.sh`, which should be modified for _each run_ and uploaded to the pvc:
```
scp config_rebuilt_runai.sh {gaspar_user}@cdhvm0002.xaas.epfl.ch:/mnt/u12632_cdh_dhlab_002_files_nfs/data/{gaspar_username}-data/        
```

Of course, the file can also be directly modified on `cdhvm0002` between runs, as well as configuration files etc.
The example script should _not be modified in this repository and pushed_ as it contains sensitive information.

**Upload your code on `cdhvm0002`**

Upload your `impresso-pycommons` folder to `cdhvm0002`. This code will be the one executed, allowing for faster changes between code versions (eg. during development), and maintaining the filestructure and paths. Hence, logs will remain on the pvc in `impresso-pycommons/impresso_commons/data/logs` even once the pod is destroyed.
Remember however to update the docker image with significant or impactful code changes.
```
scp -r impresso-pycommons {gaspar_user}@cdhvm0002.xaas.epfl.ch:/mnt/u12632_cdh_dhlab_002_files_nfs/data/{gaspar_username}-data/        
```

### Step 4: Sumbit the RunAi job

**1. Launch the Runai job**
```
runai submit --name {job_name} --image ic-registry.epfl.ch/dhlab/impresso_pycommons:{version_to_use} --pvc runai-dhlab-{gaspar_username}-data1:/home/{gaspar_username}/dhlab-data --environment USER_NAME={gaspar_username} --environment USER_ID={id}
```

The `USER_ID` can be found on [people.epfl](https://people.epfl.ch/) under `UID`.

You can monitor the job creation by running
```
runai describe job {job_name} -p dhlab-{gaspar_username}
```

**2. Enable port forwarding**
Once the job has successfully started, in another terminal window (optionally in a screen based on the expected runtime), enable port forwarding to have access to the Dask dashboard at `http://localhost:8787/status`. 
(Note: when multiple jobs run at the same time, the port needs to be adapted).
```
kubectl port-forward {job_name}-0-0 8787:8787         
```

**3. Launch the script from the pod**
You can connect to a bash session on the pod using:
```
runai exec -it {job_name} /bin/bash           
```

Then, the script can directly be launched with:
```
. scripts/start_rebuilt_runai.sh  
```

Note: The docker image can also easily be modified to directly run the script upon start. It was not added to the current Dockerfile to allow for slightly more control and a possibility to modify scripts on the pod if necessary.