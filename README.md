# oxeo-flows
🤟 Sorry, renamed the repo again...

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
  --tag=eu.gcr.io/oxeo-main/oxeo-flows \
  --ignore-file=.dockerignore
```

Or using the `cloudbuild.yaml`:
```
gcloud builds submit --config=cloudbuild.yaml .
```

Or locally:
```
docker build . -t oxeo-flows
```

Automatically build the image from GitHub using Cloud Build triggers.
Specify `Dockerfile`, `cloudbuild.yaml` and `requirements.txt` in the "Included files".
Flows will be pulled from GitHub, so we don't need the image to rebuild with each update!

### Enable access to GCR registry
Create a Service Account with `Container Registry` permissions. This is only for the agent, so need to run before starting it.

Only need to do this when running locally:
```
gcloud auth activate-service-account \
  --key-file=path/to/token.json

gcloud auth configure-docker
```

Run the docker agent locally:
```
prefect agent docker start \
  --label=pc \
  --env PREFECT__CONTEXT__SECRETS__GITHUB=<github-token>
```

## Running on Google Vertex
To run the agent locally, pushing jobs to Vertex:
```
prefect agent vertex start \
  --label=pc \
  --project=oxeo-main \
  --region-name=europe-west4 \
  --service-account=<service-acc-email> \
  --env=PREFECT__CONTEXT__SECRETS__GITHUB=<github-token>
```

The above is in the Dockerfile `CMD`. Executed by the Agent (see below) but ignored by created `VertexRun` instances, where it is overridden.

The VertexRun instances use an image and machine type specified in `VertexRun` in the flow file.
Available image types are listed [here](https://cloud.google.com/vertex-ai/docs/training/configure-compute).

Run the Agent image on Vertex:
```
gcloud ai custom-jobs create \
 --region=europe-west4 \
 --display-name=prefect-agent-test2 \
 --service-account=prefect@oxeo-main.iam.gserviceaccount.com \
 --worker-pool-spec=machine-type=n1-highmem-2,replica-count=1,container-image-uri=eu.gcr.io/oxeo-main/oxeo-flows:latest
```

## Secrets
Service Account JSON token removed from the Dockerfile, as it should be provided automatically by the Vertex instance.

SSH key to install projects from GitHub is currently copied into the Docker image, should be provided by Google Secrets Manager:

Go to Secrets Manager, create new Secret, upload SSH private key with a name. Same for Prefect API key.

More:
- [Accessomg GitHub from a build via SSH](https://cloud.google.com/build/docs/access-github-from-build)
- [Using secrets from Secret Manager](https://cloud.google.com/build/docs/securing-builds/use-secrets)

Also need to add `Secret Manager Secret Accessor` role to the Cloud Build service account (e.g. `1234567@cloudbuild.gserviceaccount.com`).

### Summary of Secrets being used and where they are
- SSH token authorised to access Chris GitHub acc, stored in GCP Secret Manager and in GitHub secrets attached to `oxeo-flows` repo.
- Prefect API key stored in the same two places as above
- GitHub Personal Access Token (Chris acc) stored in Prefect secrets
- Service Account JSON key for sat-extractor is stored in GCP Secret Manager

# Control
And control from the web UI!
