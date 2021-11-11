import json
from concurrent import futures
from typing import Callable

from google.cloud import pubsub_v1
from typer import run


def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


def main(project_id: str, topic_id: str, msg: str) -> None:
    publisher = pubsub_v1.PublisherClient.from_service_account_file(
        "secrets/oxeo-main-prefect.json",
    )
    topic_path = publisher.topic_path(project_id, topic_id)
    # pub(publisher, topic_path, msg)
    pub2(publisher, topic_path, msg)


def pub(publisher, topic_path, msg):
    data = {"msg": msg}
    data = json.dumps(data).encode("utf-8")

    publish_futures = []
    publish_future = publisher.publish(
        topic_path,
        data,
        type="model",
    )

    print(publish_future.done())
    message_id = publish_future.result()

    print(f"Published {data} to {topic_path}: {message_id}")
    publish_futures.append(publish_future)
    futures.wait(
        publish_futures,
        return_when=futures.ALL_COMPLETED,
    )
    print(publish_future.done())


def pub2(publisher, topic_path, msg) -> None:
    publish_futures = []
    for i in range(10):
        # data = {"msg": f"{msg} {i}"}
        # data = json.dumps(data).encode("utf-8")
        data = str(i).encode("utf-8")
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, data, type="model")
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")


if __name__ == "__main__":
    run(main)
