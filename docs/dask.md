# Dask
Each individual Flow run will run on its own Google Vertex instance.
Parallelism within a Flow run can use a `LocalDaskExecutor`, which will just scale to the resources of the machine.
For larger parallelism, we need to use some kind of distributed Dask infrastructure.
There are several ways of achieving this, described below.


## Local DaskExecutor
By setting `executor=DaskExecutor()` with no arguments, it will just parallelise on the local machine.

## Dask-Kubernetes KubeCluster
This is what we're currently using.
[KubeCluster](https://kubernetes.dask.org/en/latest/kubecluster.html) creates ephemeral Dask clusters in a Kubernetes cluster.
Need to ensure that [additional packages are installed](https://github.com/dask/dask-kubernetes/issues/329#issuecomment-989697205) to get it to work, and be careful that the Docker image being used fo r the workers has the same versions.

## Dask Cloudprovider
Using [dask-cloud-provider](https://cloudprovider.dask.org/en/latest/gcp.html).
This is the default mechanism for Prefect to spin up ephemeral clusters for each Flow run, using `GCPCluster`.
This spins up bare GCP Compute instances with an OS image and then runs a Docker image from there.

To make that work, had to create a [Packer](https://cloudprovider.dask.org/en/stable/packer.html) image that runs the `oxeo-flows` Docker image.
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

## VPC Peering
**NB**: Since the Prefect Agent is now running in GKE, which sits on top of GCE (unlike Vertex), we don't need the VPC Peering stuff any more. Everything runs on the `default` network, and all that is necessary is to open ingress on `10.0.0.0/8` so that the GKE Agent can talk to the GCE Scheduler and Workers.
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
