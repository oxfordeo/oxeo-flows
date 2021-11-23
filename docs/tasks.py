import json
from os import environ

from google.cloud import tasks_v2
from typer import run

project = environ.get("PROJECT_ID")
queue = environ.get("QUEUE_NAME")
location = environ.get("LOCATION_ID")
gcr_url = environ.get("GCR_URL")
serv_ac = environ.get("SERV_AC")


def create_task(
    payload: str,
    project: str = project,
    queue: str = queue,
    location: str = location,
    gcr_url: str = gcr_url,
    serv_ac: str = serv_ac,
):
    body = json.dumps(json.loads(payload)).encode()

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    task = {
        # "dispatchDeadline": "1800s",
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{gcr_url}/api/",
            "headers": {"Content-type": "application/json"},
            "body": body,
            "oidc_token": {
                "service_account_email": serv_ac,
                "audience": gcr_url,
            },
        }
    }
    response = client.create_task(parent=parent, task=task)

    print(f"Created task {response.name}")
    return response


if __name__ == "__main__":
    run(create_task)
