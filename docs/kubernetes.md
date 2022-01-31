# Kubernetes

Using GKE.

Create an Autopilot cluster:
```
gcloud container clusters create-auto pf-cluster --region=europe-west4
```

Using Cloud Shell to load config:
```
gcloud container clusters get-credentials pf-cluster --region=europe-west4 --project=oxeo-main

# check that it worked
kubectl get nodes
```

Prefect:
```
pip3 install prefect[kubernetes]
```

Run the agent locally:
```
# key from Prefect Cloud
PREFECT_KUB_KEY=...
prefect agent kubernetes start --key=$PREFECT_KUB_KEY
```

Run the agent in cluster:
```
export PREFECT_KUB_KEY=...
prefect agent kubernetes install -k $PREFECT_KUB_KEY --rbac | \
  kubectl apply -f -
```

The command above creates a YAML config and then passes it to kubectl. However, it doesn't have sufficient RBAC permissions for the KubeCluster Dask Cluster (which will be run from the same Kubernetes service account), so there are additional permissions added in [../infra/kubernetes-agent.yaml](../infra/kubernetes-agent.yaml).

Then run as follows (*instead of the command above!*). (This uses `sed` to drop the Prefect key into the required spot.)
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

## Manual cluster
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
  --machine-type e2-standard-8 \
  --image-type COS_CONTAINERD \
  --node-labels type=big \
  --metadata disable-legacy-endpoints=true \
  --enable-autoscaling \
  --min-nodes 1 \
  --max-nodes 10 \
  --enable-autoupgrade \
  --enable-autorepair \
  --max-surge-upgrade 1 \
  --max-unavailable-upgrade 0 \
  --max-pods-per-node 16 \
  --node-locations europe-west4-b
```

```bash
gcloud container node-pools create gpu \
  --project oxeo-main \
  --cluster oxeo-cluster \
  --zone europe-west4-b \
  --machine-type n1-standard-8 \
  --accelerator type=nvidia-tesla-t4,count=1 \
  --image-type COS_CONTAINERD \
  --node-labels type=gpu \
  --metadata disable-legacy-endpoints=true \
  --enable-autoscaling \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoupgrade \
  --enable-autorepair \
  --max-surge-upgrade 1 \
  --max-unavailable-upgrade 0 \
  --max-pods-per-node 8 \
  --node-locations europe-west4-b
```
