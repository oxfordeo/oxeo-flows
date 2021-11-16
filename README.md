# oxeo-flows
Repository for managing pipelines etc, using Prefect for orchestration.

## Prefect
Log in at [Prefect Cloud](https://cloud.prefect.io/) and get an API key from the user account page (different from an agent/project key).

## Install
```
pip install -r requirements.txt
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
Create a Service Account with `Container Registry` permissions.
This is only for the agent, so need to run before starting it.

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

The above is in the Dockerfile `CMD`.
Executed by the Agent (see below) but ignored by created `VertexRun` instances, where it is overridden.

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
To get the secrets out of the Dockerfile/containing environment, they have been placed in Google Secret Manager, GitHub Secrets and Prefect Secrets.

### Build-time secrets
These are [injected from Google Secret Manager](https://cloud.google.com/build/docs/securing-builds/use-secrets), via the [cloudbuild.yaml](./cloudbuild.yaml) config. Relies on some hacky `sed`ing to get the SSH keys to work.
Also need to add `Secret Manager Secret Accessor` role to the Cloud Build service account (e.g. `1234567@cloudbuild.gserviceaccount.com`).
- SSH token authorised on Oxeo GitHub account: to download GitHub dependencies
- Prefect API key: to connect to Prefect Cloud
- Google Service Account JSON token: to use the Google SDK within the image

### Run-time secrets
The same as above, in addition the following, stored on Prefect Secrets.
- GitHub Personal Access Token linked to Oxeo GitHub account: for Prefect to access the Flows from GitHub Storage

### Prefect Registration secrets
Stored on GitHub Secrets, used in the GitHub Action that registers updated Flows with Prefect Cloud.
- Same SSH token: download GitHub dependencies to install requirements before registration
- Prefect API key: to register the Flows with Prefect Cloud

# Dask
Each individual Flow run will run on its own Google Vertex instance.
Parallelism within a Flow run can use a `LocalDaskExecutor`, which will just scale to the resources of the machine.
For larger parallelism, we need to use some kind of distributed Dask infrastructure.
There are several ways of achieving this, described below.

## VPC Peering
All of these solutions rely on Vertex talking to resources in Google Compute.
As Vertex is not automatically part of the Compute VPC, it needs to be peered and/or the VPC must allow public ingress on the Dask ports.

The solution currently used is as follows:
1. Create a new VPC network (called `dask`) with one subnetwork on europe-west4 (also called `dask`).
2. Add Firewall rules for *this network* [as follows](https://cloudprovider.dask.org/en/stable/gcp.html):
```
egress 0.0.0.0/0 all ports
ingress 10.0.0.0/8 all ports (internal communication)
ingress 0.0.0.0/0 on 8786-8787 (external accessibility to scheduler)
```
4. [Set up a VPC peering](https://cloud.google.com/vertex-ai/docs/general/vpc-peering).
5. Then when creating Vertex instances, specify that they should connect to the `dask` network/subnetwork.

This is still relying on the public ingress, which shouldn't be necessary, as the networks are peered.
`dask-cloudprovider` (and probably the others below have similar) has a `public_ingress=False` option, which will then tell the client (the Vertex instance) to use the internal IP address.
However, this option is not exposed in Prefect, so we have to rely on the public IP.
This is potentially not ideal from a security POV (not important for ephemeral instances, and limited to the `dask` network), and has [cost implications](https://cloud.google.com/vpc/network-pricing).
There is some [discussion](https://github.com/dask/dask-cloudprovider/issues/161) [around](https://github.com/dask/dask-cloudprovider/issues/215) [this](https://github.com/dask/dask-cloudprovider/issues/229) issue.

## Local DaskExecutor
By setting `executor=DaskExecutor()` with no arguments, it will just parallelise on the local machine.

## Dask Cloudprovider
Using [dask-cloud-provider](https://cloudprovider.dask.org/en/latest/gcp.html).
This is the default mechanism for Prefect to spin up ephemeral clusters for each Flow run, using `GCPCluster`.
This spins up bare GCP Compute instances with an OS image and then runs a Docker image from there.

To make that work, had to create a Packer image that runs the `oxeo-flows` Docker image.
See [the config](./packer/packer.json) and build a new image by running `packer build packer.json` and get the packer image ID from the output.
This name must be specified in the `GCPCluster` config.

It's several minutes before things start happening, as needs to first start up the Vertex, and then get the cluster going.

Code required in the Flow is as follows, making sure to specify the correct network (eg `dask` and Packer image).
```
executor = DaskExecutor(
    cluster_class=GCPCluster,
    adapt_kwargs={...},
    cluster_kwargs={...},
)
```

## Dask's Helm charts
Instead of ephemeral instances, [Helm](https://helm.dask.org/) can be used to have a longer-running cluster on Google Kubernetes Engine (GKE).
Followed [these instructions](https://towardsdatascience.com/scalable-machine-learning-with-dask-on-google-cloud-5c72f945e768) to get started.
[Final working config is here](./helm-chart-dask.yaml).

There were some [timezone issues](https://github.com/pangeo-data/pangeo-docker-images/issues/125) with the Dask/Prefect combo, solved by setting `TZ=UTC` and/or adding `tzdata` to the Helm config.
Helm works well, but doesn't provide scaling!

Do the following in Google Cloud Shell:
```
gcloud container clusters create \
  dask-cluster \
  --machine-type=n1-standard-4 \
  --num-nodes=2 \
  --zone=europe-west4-a  \
  --cluster-version=latest \
  --network=dask \
  --subnetwork=dask

kubectl create clusterrolebinding \
  cluster-admin-binding \
  --clusterrole=cluster-admin \
  --user=<google-user-email-address>
```

Setup Helm (still in Cloud Shell):
```
curl https://raw.githubusercontent.com/helm/helm/HEAD/scripts/get-helm-3 | bash

helm repo add dask https://helm.dask.org/
helm repo update

helm install dask-cluster dask/dask -f helm-chart-dask.yaml
```

See info about running services or pods:
```
kubectl get services
kubectl get pods
```

Then to use the cluster from Prefect, get the external IP and port for the LoadBalancer service (from commands above) and use as follows:
```
executor=DaskExecutor(address="tbp://<ip>:<port>")
```

## Dask-Kubernetes KubeCluster
[KubeCluster](https://kubernetes.dask.org/en/latest/kubecluster.html) provides a long-lived Dask cluster, that also scales. Make sure to `pip install dask-kubernetes==2021.3.1` as there is an [issue](https://github.com/dask/dask-kubernetes/issues/376) with the latest version.
Haven't quite got this working, but not sure we need it yet!

## Dataproc
Tried to create a custom image for Dataproc using [these](https://cloud.google.com/dataproc/docs/guides/dataproc-images) instructions and a bash script based on the Dockerfile, but couldn't get it to play nice with SSH pip install.

[Script is here](./gcloud/dataproc-dask-install.sh), then run:
```
python generate_custom_image.py \
  --image-name=dataproc-dask \
  --dataproc-version=2.0.24-ubuntu18 \
  --customization-script=dask.sh \
  --zone=europe-west4-a \
  --gcs-bucket=gs://oxeo-dataproc-custom-image \
  --disk-size=30
```

# Control
And control from the [web UI](https://cloud.prefect.io/)!
