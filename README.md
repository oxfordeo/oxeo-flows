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
prefect auth login --key-file <your-api-key>
```

(If you get authentication errors, you may need to delete `~/.prefect/`.)

### Run a flow locally
This works even if the Flow is set up with VertexRun etc.
```
prefect run -p flows/extract.py \
  --param aoi=aoi.geojson \
  --param credentials=token.json
```

### Register a flow
```
prefect register --project <proj-name> -p flows/extract.py
```

### Start the agent to listen for jobs
```
prefect agent local start
```

## Docker agent setup
First need to [authorize access to GCS container registry](https://cloud.google.com/container-registry/docs/advanced-authentication#gcloud-helper).

### Generate SSH key
```
ssh-keygen -t ed25519 -f keys/deploy_key
```
And add it to GitHub.

### Create image in GCR registry
```
gcloud builds submit . \
  --tag=gcr.io/oxeo-main/oxeo-flows \
  --ignore-file=.dockerignore
```

Or locally:
```
docker build . -t oxeo-flows
```

### Enable access to GCR registry
Create a Service Account with `Container Registry` permissions. This is only for the agent, so need to run before starting it.
```
gcloud auth activate-service-account \
  --key-file=path/to/token.json

gcloud auth configure-docker
```

```
prefect agent docker start \
  --label=pc \
  --env PREFECT__CONTEXT__SECRETS__GITHUB=<github-token>
```

## Running on Google Vertex
```
prefect agent vertex start \
  --label=pc \
  --project=oxeo-main \
  --region-name=europe-west4 \
  --service-account <service-acc-email> \
  --env PREFECT__CONTEXT__SECRETS__GITHUB=<github-token>
```

Create image with above:
```
gcloud builds submit agent/ \
  --tag=gcr.io/oxeo-main/prefect-agent \
  --ignore-file=.dockerignore
```

# Control
And control from the web UI!
