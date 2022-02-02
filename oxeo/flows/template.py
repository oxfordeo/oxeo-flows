from datetime import timedelta
from typing import List

import prefect
from prefect import Flow, Parameter, task
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

import oxeo.flows.config as cfg
from oxeo.flows.utils import rename_flow_run


# More task parameters at
# https://docs.prefect.io/api/latest/core/task.html
@task(log_stdout=True, max_retries=1, retry_delay=timedelta(seconds=10))
def task_one(
    a_word: str,
    # Make sure to provide return type annotations
) -> List[int]:

    # Prefect logger sends messages to cloud
    # https://docs.prefect.io/core/concepts/logging.html
    logger = prefect.context.get("logger")
    logger.info("Running task one...")

    # Do whatever you want in here
    # Run libraries, shell out to gcloud, wait for
    # stuff to happen...

    numbers = list(range(len(a_word)))
    return numbers


@task(log_stdout=True)
def task_two(
    number: int,
) -> None:
    # More context here:
    # https://docs.prefect.io/api/latest/utilities/context.html
    logger = prefect.context.get("logger")
    logger.info("Running task two...")

    # Tasks don't have to return anything
    # If they don't error, Prefect will assume
    # they were successful
    logger.info(number)


# Each flow needs the following:
# - name: change it to uniquely identify the flow!
# - executor: where the *Tasks* are run
# - storage: where the task code comes from
# - run_config: where the *Flow* is run

# Empty DaskExecutor() spins up a local Dask
# cluster and parallelises across that
# e.g. if two tasks don't depend on each other,
# they can be run at the same time
# See predict.py for the config of a remote cluster
executor = DaskExecutor()

# We're using GitHub storage
# Prefect Cloud doesn't actually have the Flow code
# just the metadata, parameters etc
# So when a flow is run, it pulls the GitHub code
# needed at that moment
# Dependencies are baked into the Docker image!
storage = GitHub(
    repo=cfg.repo_name,
    path="oxeo/flows/predict.py",
    access_token_secret=cfg.prefect_secret_github_token,
)

# This specifies where the Flow is run.
# Kubernetes for us. If you use a remote Dask cluster,
# Only the Flow *glue* will run on Kubernetes, and all
# the actual tasks will run on the cluster.
# If you don't want to use a cluster, you can instead
# just specify a beefier machine here!
run_config = KubernetesRun(
    image=cfg.docker_oxeo_flows,
)
with Flow(
    # don't forget to change this!
    "template",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # Parameters will show up in Prefect Cloud
    aoi_id = Parameter(name="aoi_id", required=True)
    word = Parameter(name="word", default="Hello")

    # This is just a task to rename the Flow run
    # (that shows up in Cloud after running)
    # to something more useful!
    rename_flow_run(aoi_id)

    # Prefect uses the code to figure out the implicit
    # dependency graph between tasks
    # eg rename_flow_run() will just depend on the parameters
    # and not be linked to the two below
    list_of_numbers = task_one(word)

    # .map() maps the Task across the supplied list
    # So these will be parallelised across whatever system is being used
    task_two.map(list_of_numbers)
