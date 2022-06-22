import geopandas as gpd
from prefect import Flow, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


@task
def foo():
    return "hello"


@task(log_stdout=True)
def bar(x):
    print(x + " world")
    print(gpd.__version__)


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/test-aws.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image="413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest",
    )

    with Flow(
        "test-aws",
        storage=storage,
        run_config=run_config,
    ) as flow:
        word = foo()
        bar(word)

    return flow


flow = create_flow()
