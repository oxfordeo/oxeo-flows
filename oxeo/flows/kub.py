from typing import List

import prefect
from prefect import Flow, Parameter, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

from oxeo.flows import docker_oxeo_flows, prefect_secret_github_token, repo_name


@task
def task_one(
    a_word: str,
) -> List[int]:

    logger = prefect.context.get("logger")
    logger.info("Running task one...")

    numbers = list(range(len(a_word)))
    return numbers


@task
def task_two(
    number: int,
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Running task two...")
    logger.info(number)


storage = GitHub(
    repo=repo_name,
    path="oxeo/flows/kub.py",
    access_token_secret=prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=docker_oxeo_flows,
)
with Flow(
    "kub",
    storage=storage,
    run_config=run_config,
) as flow:
    word = Parameter(name="word", default="Hello")
    list_of_numbers = task_one(word)
    task_two.map(list_of_numbers)
