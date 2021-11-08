import prefect
from prefect import task, Flow, Parameter
from prefect.client import Client
from prefect.tasks.shell import ShellTask


@task
def rename_flow_run(lake):
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_name = f"run_{lake}"
    client = Client()
    return client.set_flow_run_name(flow_run_id, flow_run_name)


@task
def say_hello(lake):
    logger = prefect.context.get("logger")
    logger.info(f"Hello, Cloud!, do {lake}")
    with open("output.txt", "a") as f:
        print(f"Hello, Cloud, do {lake}", file=f)


ls = ShellTask(command="ls", return_all=True)
shell = ShellTask()


def create_flow():
    with Flow("hello-flow") as flow:
        lake = Parameter(name="lake", required=True)
        rename_flow_run(lake)
        say_hello(lake)
        ls_count = ls(command="ls | wc -l")
        shell(command=f"echo lake {lake} {ls_count}")
    return flow


if __name__ == "__main__":
    flow = create_flow()
    flow.register(project_name="test")
    # flow.run()
    # flow.visualize()
