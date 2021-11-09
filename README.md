# oxeo-pipes
Repository for managing pipelines etc, using Prefect for orchestration.

Look in [gcloud/](gcloud/) to see some sample scripts and commands specific to Google Cloud.

## Prefect
https://docs.prefect.io/core/getting_started/install.html

Log in at [Prefect Cloud](https://cloud.prefect.io/) and get an API key from the user account page (different from an agent/project key).

## Install
```
mkvirtualenv prefect
pip install -r requirements.txt
```

May need to install Graphviz to visualize DAGs locally:
```
sudo apt install graphviz
```

## Getting started
Tell core that we're working with Cloud and authenticate:
```
prefect backend cloud
prefect auth login --key <your-api-key>
```

(If you get authentication errors, you may need to delete `~/.prefect/`.)

### Run a flow locally
```
prefect run -p flows/extract.py
```

### Register a flow
```
prefect register --project <proj-name> -p flows/extract.py
```

### Start the agent to listen for jobs
```
prefect agent local start
```

And control from the web UI!
