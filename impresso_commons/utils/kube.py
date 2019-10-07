import os


DEFAULT_PIP_PACKAGES = [
    # "https://github.com/impresso/impresso-pycommons/archive/master.zip"
]


def make_scheduler_configuration():
    scheduler_pod_spec_yaml = """
      containers:
        - image: ic-registry.epfl.ch/dhlab/impresso_pycommons:v1
          command: ["sh", "-c"]
          args:
            - dask-scheduler --port 8786 --bokeh-port 8787
          imagePullPolicy: Always
          name: dask-scheduler
          ports:
            - containerPort: 8787
            - containerPort: 8786
          resources:
            requests:
              cpu: 1
              memory: 4G
    """
    return scheduler_pod_spec_yaml


def make_worker_configuration(
    docker_image,  # TODO: rename docker_image
    memory="1G",
    extra_pip_packages=DEFAULT_PIP_PACKAGES
):

    s3_access_key = os.environ["SE_ACCESS_KEY"]
    s3_secret_key = os.environ["SE_SECRET_KEY"]

    config_template = f"""
      containers:
        - image: {docker_image}
          args: [dask-worker, $(DASK_SCHEDULER_ADDRESS), --nthreads, '1', --no-bokeh, --memory-limit, {memory}, --death-timeout, '120']
          imagePullPolicy: Always
          name: dask-worker
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: EXTRA_PIP_PACKAGES
              value: {" ".join(extra_pip_packages)}
            - name: EXTRA_CONDA_PACKAGES
              value:
            - name: SE_ACCESS_KEY
              value: {s3_access_key}
            - name: SE_SECRET_KEY
              value: {s3_secret_key}
          resources:
            requests:
              cpu: 1
              memory: {memory}
            limits:
              cpu: 1
              memory: {memory}
          volumeMounts:
            - mountPath: /scratch
              name: scratch
              subPath: romanell
      volumes:
        - name: scratch
          persistentVolumeClaim:
            claimName: dhlab-scratch
    """ # noqa

    return config_template
