import numpy as np
import prefect
from dask_cloudprovider.gcp import GCPCluster
from prefect import Flow, Parameter, task
from prefect.client import Client
from prefect.executors import DaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GitHub


@task
def rename_flow_run(
    aoi_id: int,
) -> None:
    logger = prefect.context.get("logger")
    old_name = prefect.context.get("flow_run_name")
    new_name = f"run_{aoi_id}"
    logger.info(f"Original flow run name: {old_name}")
    logger.info(f"Rename the Flow Run to {new_name}")
    Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)


@task
def generate_vals():
    logger = prefect.context.get("logger")
    logger.info("Generating 10 random floats")
    return np.random.rand(10)


@task
def burn(val):
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Start loop creating arrays on task: {task_full_name}")
    for a in range(int(0.5 * 2000)):
        if a % 500 == 0:
            logger.info(f"This is index: {a} on {task_full_name}")
        np.random.rand(a, a) ** 2


executor = DaskExecutor(
    cluster_class=GCPCluster,
    adapt_kwargs={"maximum": 10},
    cluster_kwargs={
        # "n_workers": 4,
        "projectid": "oxeo-main",
        "source_image": "packer-1636725840",
        "docker_image": "eu.gcr.io/oxeo-main/oxeo-flows:latest",
        "zone": "europe-west4-a",
        "machine_type": "n1-highmem-2",
    },
)
executor = DaskExecutor(address="tcp://34.90.5.193:8786")
storage = GitHub(
    repo="oxfordeo/oxeo-flows",
    path="flows/predict.py",
    access_token_secret="GITHUB",
)
run_config = VertexRun(
    labels=["vertex"],
    image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
    machine_type="n1-highmem-2",
    network="projects/292453623103/global/networks/default",
)
with Flow(
    "predict",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # parameters
    aoi_id = Parameter(name="aoi_id", required=True)
    rename_flow_run(aoi_id)

    vals = generate_vals()
    burn.map(vals)
