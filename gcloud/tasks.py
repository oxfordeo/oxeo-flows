from os import environ
import json

from google.cloud import tasks_v2
from typer import run


def create_task(
    payload: str,
    project: str = environ.get("PROJECT_ID"),
    queue: str = environ.get("QUEUE_NAME"),
    location: str = environ.get("LOCATION_ID"),
    gcr_url: str = environ.get("GCR_URL"),
    serv_ac: str = environ.get("SERV_AC"),
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

    print("Created task {}".format(response.name))
    return response


if __name__ == "__main__":
    run(create_task)
