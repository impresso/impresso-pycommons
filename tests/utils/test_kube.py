from dask_k8 import DaskCluster

from impresso_commons.utils.kube import (make_scheduler_configuration,
                                         make_worker_configuration)


def test_make_kube_config():
    assert make_worker_configuration(
        docker_image="ic-registry.epfl.ch/dhlab/impresso_pycommons:v1"
    ) is not None


def test_dask_cluster():
    cluster = DaskCluster(
        namespace="dhlab",
        cluster_id="impresso-pycommons-test",
        scheduler_pod_spec=make_scheduler_configuration(),
        worker_pod_spec=make_worker_configuration(
            docker_image="ic-registry.epfl.ch/dhlab/impresso_pycommons:v1"
        )
    )
    #import ipdb; ipdb.set_trace()
    cluster.create()
    cluster.scale(2, blocking=True)
    dask_client = cluster.make_dask_client()
    print(dask_client)
    #import ipdb; ipdb.set_trace()
    cluster.close()
