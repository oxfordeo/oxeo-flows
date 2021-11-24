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
prefect agent kubernetes install -k $PREFECT_KUB_KEY --rbac | kubectl apply -f -
```

Watch with:
```
kubectl get all
kubectl get deploy
kubectl describe deploy
```
