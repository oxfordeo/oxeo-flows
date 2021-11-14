PROJECT_ID=oxeo-main
gcloud config set project $PROJECT_ID

REGION=europe-west4

# This is for display only; you can name the range anything.
PEERING_RANGE_NAME=dask-reserved-range

NETWORK=dask

# NOTE: `prefix-length=16` means a CIDR block with mask /16 will be
# reserved for use by Google services, such as Vertex AI.
gcloud compute addresses create $PEERING_RANGE_NAME \
  --global \
  --prefix-length=16 \
  --description="peering range for Google service" \
  --network=$NETWORK \
  --purpose=VPC_PEERING

# Create the VPC connection.
gcloud services vpc-peerings connect \
  --service=servicenetworking.googleapis.com \
  --network=$NETWORK \
  --ranges=$PEERING_RANGE_NAME \
  --project=$PROJECT_ID
