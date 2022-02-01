import prefect
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, task
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

import oxeo.flows.config as cfg


@task
def check_cuda() -> None:
    import torch

    logger = prefect.context.get("logger")
    cuda = torch.cuda.is_available()
    logger.info(f"CUDA available: {cuda}")

    import zarr
    import numpy as np

    m = zarr.open_array("", "a", shape=(1, 1), chunks=(1, 1), dtype=np.uint8)
    logger.info(f"Zarr ran fine: {m}")


def dynamic_cluster(**kwargs):
    n_workers = 1
    memory = "8G"
    cpu = 1
    gpu = 1
    container_config = {
        "resources": {
            "limits": {
                "cpu": cpu,
                "memory": memory,
                "nvidia.com/gpu": gpu,
            },
            "requests": {
                "cpu": cpu,
                "memory": memory,
                "nvidia.com/gpu": gpu,
            },
        }
    }
    pod_spec = make_pod_spec(
        image=cfg.docker_oxeo_flows_gpu,
        extra_container_config=container_config,
    )
    pod_spec.spec.containers[0].args.append("--no-dashboard")
    return KubeCluster(n_workers=n_workers, pod_template=pod_spec, **kwargs)


executor = DaskExecutor(
    cluster_class=dynamic_cluster,
    cluster_kwargs={},
)
storage = GitHub(
    repo=cfg.repo_name,
    path="oxeo/flows/test-cuda.py",
    access_token_secret=cfg.prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=cfg.docker_oxeo_flows_gpu,
)
with Flow(
    "test-cuda",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    done = check_cuda()
