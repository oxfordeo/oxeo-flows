# Kubernetes

## GPU cluster
Autopilot doesn't support GPUs, so we must create a manual cluster, with 2-3 node pools:
- small machines (for agent, Flow runs)
- big machines (for Dask runs)
- GPU machines (for DL stuff)

Instructions below are for a cluster with just two pools (big and GPU).

```bash
gcloud container clusters create oxeo-cluster \
  --project oxeo-main \
  --zone europe-west4-b \
  --release-channel regular \
  --logging=NONE \
  --monitoring=NONE \
  --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver \
  --no-enable-master-authorized-networks \
  --enable-ip-alias \
  --machine-type e2-standard-2 \
  --image-type COS_CONTAINERD \
  --node-labels type=small \
  --enable-autoscaling \
  --min-nodes 1 \
  --max-nodes 100 \
  --enable-autoupgrade \
  --enable-autorepair \
  --max-pods-per-node 16 \
  --node-locations europe-west4-b
```

```bash
gcloud container node-pools create big \
  --project oxeo-main \
  --cluster oxeo-cluster \
  --zone europe-west4-b \
  --machine-type e2-standard-16 \
  --image-type COS_CONTAINERD \
  --node-labels type=big \
  --enable-autoscaling \
  --min-nodes 0 \
  --max-nodes 100 \
  --enable-autoupgrade \
  --enable-autorepair \
  --max-pods-per-node 16 \
  --node-locations europe-west4-b
```

```bash
gcloud container node-pools create gpu \
  --project oxeo-main \
  --cluster oxeo-cluster \
  --zone europe-west4-b \
  --machine-type n1-standard-16 \
  --accelerator type=nvidia-tesla-t4,count=1 \
  --image-type COS_CONTAINERD \
  --node-labels type=gpu \
  --enable-autoscaling \
  --min-nodes 0 \
  --max-nodes 100 \
  --enable-autoupgrade \
  --enable-autorepair \
  --max-pods-per-node 8 \
  --node-locations europe-west4-b
```

Using Cloud Shell to load config:
```
gcloud container clusters get-credentials oxeo-cluster \
  --zone=europe-west4-b --project=oxeo-main

# check that it worked
kubectl get nodes
```

Don't forget to [install Nvidia drivers](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#installing_drivers) on the GPU nodes!
```
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml
```

Run the agent locally to test:
```
pip3 install prefect[kubernetes]    # if needed

# key from Prefect Cloud
PREFECT_KUB_KEY=...
prefect agent kubernetes start --key=$PREFECT_KUB_KEY
```

To run the Agent on the cluster, the command `prefect agent kubernetes install --rbac` produces the relevant yaml. However, it doesn't have sufficient RBAC permissions for the KubeCluster Dask Cluster (which will be run from the same Kubernetes service account), so there are additional permissions added in [../infra/kubernetes-agent.yaml](../infra/kubernetes-agent.yaml).

Then run as follows (*instead of the command above!*).
(This uses `sed` to drop the Prefect key into the required spot.)
```
export PREFECT_KUB_KEY=...
cat infra/kubernetes-agent.yaml \
  | sed "s/API_KEY_HERE/$PREFECT_KUB_KEY/g" \
  | kubectl apply -f -
```

Watch with:
```
kubectl get all
kubectl get deploy
kubectl describe deploy
```

## Auto-provisioning
**Not currently working.**
(Fails to allocate pods because says GPU limits haven't been defined even though they have.)
```bash
gcloud container clusters create oxeo-cluster \
  --project=oxeo-main \
  --zone=europe-west4-b \
  --addons=HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver \
  --no-enable-master-authorized-networks \
  --enable-ip-alias \
  --image-type COS_CONTAINERD \
  --enable-autoscaling \
  --enable-autoprovisioning \
  --min-cpu=0 --min-memory=0 --max-cpu=500 --max-memory=1000 \
  --min-accelerator type=nvidia-tesla-t4,count=0 \
  --max-accelerator type=nvidia-tesla-t4,count=20 \
  --min-nodes=0 --max-nodes=500 \
  --autoprovisioning-locations=europe-west4-b \
  --autoprovisioning-image-type=cos_containerd
```
