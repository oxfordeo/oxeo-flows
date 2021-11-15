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
    new_name = f"run_{aoi_id}"
    logger.info(f"Rename the Flow Run to {new_name}")
    # This fails when doing a local run to a Distributed Dask cluster
    # Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)


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
        "projectid": "oxeo-main",
        "zone": "europe-west4-a",
        "network": "dask",
        "machine_type": "n1-highmem-2",
        "source_image": "packer-1636915174",
        "docker_image": "eu.gcr.io/oxeo-main/oxeo-flows:latest",
    },
)
# executor = DaskExecutor(address="tcp://35.204.252.202:8786")
# executor = DaskExecutor()
storage = GitHub(
    repo="oxfordeo/oxeo-flows",
    path="flows/predict.py",
    access_token_secret="GITHUB",
)
run_config = VertexRun(
    labels=["vertex"],
    image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
    machine_type="n1-highmem-2",
    network="projects/292453623103/global/networks/dask",
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
