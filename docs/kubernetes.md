# Kubernetes

Using GKE.

Create an Autopilot cluster:
```
gcloud container clusters create-auto pf-cluster \
  --region=europe-west4 \
  --network=dask \
  --subnetwork=dask
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
