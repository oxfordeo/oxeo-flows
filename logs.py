from google.cloud import logging


def list_entries(logger_name):
    """Lists the most recent entries for a given logger."""
    logging_client = logging.Client()
    logger = logging_client.logger(logger_name)

    print(f"Listing entries for logger {logger_name}:")

    entries = []
    for entry in logger.list_entries():
        timestamp = entry.timestamp.isoformat()
        entries.append((timestamp, entry.payload))
    return entries


def print_entries(entries):
    for timestamp, payload in entries:
        task = payload["task"].split("/")[-1]
        keys = payload.keys()
        if "taskCreationLog" in keys:
            action = "tasked"
        elif "attemptDispatchLog" in keys:
            action = "dispatched"
        elif "attemptResponseLog" in keys:
            action = "responded"
        else:
            raise NotImplementedError(f"Found a different payload: {keys}")
        print(timestamp, task, action)


if __name__ == "__main__":
    entries = list_entries("cloudtasks.googleapis.com%2Ftask_operations_log")
    print_entries(entries)
