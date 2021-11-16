import prefect
from prefect import task
from prefect.client import Client


@task(max_retries=0)
def rename_flow_run(
    aoi_id: int,
) -> None:
    logger = prefect.context.get("logger")
    old_name = prefect.context.get("flow_run_name")
    new_name = f"run_{aoi_id}"
    logger.info(f"Rename the Flow Run from {old_name} to {new_name}")
    Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)
