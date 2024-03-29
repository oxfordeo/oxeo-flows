# Google Cloud Run, PubSub, Tasks and Logging

## Cloud Run
Look in [run](./run/) directory.
### Local dev
```
pip install -r requirements.txt
FLASK_DEBUG=True FLASK_APP=web.py flask run -p 8000
```

Test:
```
curl -X POST http://127.0.0.1:8000/api/ \
  -H 'Content-Type: application/json' \
  -d '{"message": {"data": {"msg": "Hello!"}}}'
```

### Buildpack
Install Buildpack:
```
sudo add-apt-repository ppa:cncf-buildpacks/pack-cli
sudo apt install pack-cli
```

Run Locally with Buildpacks & Docker:
```
pack build --path=run/ --builder=gcr.io/buildpacks/builder chris-test
docker run -it -ePORT=8080 -p8080:8080 chris-test
```
Run the following (can run again, will automatically overwrite previous version):
```
gcloud run deploy chris-test --source=run/ \
  --region=europe-west4 --no-allow-unauthenticated \
  --memory=4G --timeout=15m --platform=managed \
  --concurrency=80
```

Add service worker permission to launch from PubSub:
```
export SERV_AC='prefect@oxeo-main.iam.gserviceaccount.com'

gcloud run services add-iam-policy-binding chris-test \
  --role=roles/run.invoker --region=europe-west4 \
  --member=serviceAccount:$SERV_AC
```

Get endpoint:
```
export GCR_URL=$(gcloud run services describe chris-test \
  --platform=managed --region=europe-west4 \
  --format='value(status.url)')
```

## Pub/Sub
Look at [pub.py](./pub.py) file.
Links:
[1](https://cloud.google.com/pubsub/docs/building-pubsub-messaging-system)
[2](https://github.com/googleapis/python-pubsub/tree/main/samples/snippets/quickstart)
[3](https://cloud.google.com/pubsub/docs/samples/pubsub-publisher-retry-settings)

Max ack deadline is 10 minutes.

Create topic:
```
gcloud pubsub topics create chris-test
gcloud pubsub topics create chris-test-dlq
```

Create subscription with filter
```
gcloud pubsub subscriptions create chris-test \
  --topic='projects/oxeo-main/topics/chris-test' \
  --push-endpoint=$GCR_URL/api/ \
  --message-filter='attributes.type = "model"' \
  --push-auth-service-account=$SERV_AC \
  --ack-deadline=600 \
  --dead-letter-topic=chris-test-dlq \
  --max-delivery-attempts=5 \
  --min-retry-delay=10s \
  --max-retry-delay=5m
```

And DLQ sub:
```
gcloud pubsub subscriptions create chris-test-dlq \
  --topic='projects/oxeo-main/topics/chris-test-dlq'
```

Get proj number and service account:
```
export PROJ_NUMBER=$(gcloud projects list \
--filter="$(gcloud config get-value project)" \
--format="value(PROJECT_NUMBER)")

export PUBSUB_SERV_AC="service-$PROJ_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com"
```

DLQ permisssions:
```
gcloud pubsub topics add-iam-policy-binding chris-test-dlq \
  --member="serviceAccount:$PUBSUB_SERV_AC" \
  --role=roles/pubsub.publisher

gcloud pubsub subscriptions add-iam-policy-binding chris-test \
  --member="serviceAccount:$PUBSUB_SERV_AC" \
  --role=roles/pubsub.subscriber
```

Send a message with appropriate attribute for filter:
```
gcloud pubsub topics publish chris-test \
  --message='{"msg": "This will apear in Cloud Run logs!"}' \
  --attribute='type=model'
```

This message will fail (because the app wants JSON in the message body) and go to DLQ:
```
gcloud pubsub topics publish chris-test \
  --message="This will fail, and retry five times" \
  --attribute='type=model'
```

This message won't be subscribed because it has a different attribute:
```
gcloud pubsub topics publish chris-test \
  --message='{"msg": "This won't be picked up by any subscriber"}' \
  --attribute='type=foobar'
```

Pull failed messages with:
```
gcloud pubsub subscriptions pull chris-test-dlq
```

## Tasks
- https://cloud.google.com/tasks/docs/dual-overview

Max timeout is 30 minutes. Can be greater with AppEngine... Set in [JSON request body](https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks).

Create queue:
```
gcloud tasks queues create chris-queue \
  --location=europe-west2 \
  --log-sampling-ratio=1 \
  --max-attempts=2 --max-backoff=60s
```

Check details:
```
gcloud tasks queues describe chris-queue
```

Set vars:
```
export PROJECT_ID="oxeo-main"
export LOCATION_ID="europe-west2"
export QUEUE_ID="chris-queue"
```

Send an HTTP task to the Cloud Run endpoint:
```
gcloud tasks create-http-task "my-task-name-12345" \
  --queue=chris-queue \
  --url="$GCR_URL/api/" \
  --location=$LOCATION_ID \
  --header='Content-Type:application/json' \
  --body-content='{"message": {"data": {"msg": "Hello!"}}}' \
  --oidc-service-account-email=$SERV_AC
```

Same, using Python:
```
python tasks.py \
  '{"message": {"data": {"msg": "hello from tasks script attempt 2!"}}}'
```

Check status (errors once task is done):
```
gcloud tasks describe --queue=chris-queue "$TASK_ID"
```

## Logging
Look at [logs.py](./logs.py) file.
```
gcloud logging logs list

gcloud logging read "logName=projects/oxeo-main/logs/cloudtasks.googleapis.com%2Ftask_operations_log"
```

Python:
```
python logs.py
```

## Dataproc
Tried to create a custom image for Dataproc using [these](https://cloud.google.com/dataproc/docs/guides/dataproc-images) instructions and a bash script based on the Dockerfile, but couldn't get it to play nice with SSH pip install.

[Script is here](dataproc-dask-install.sh), then run:
```
python generate_custom_image.py \
  --image-name=dataproc-dask \
  --dataproc-version=2.0.24-ubuntu18 \
  --customization-script=dask.sh \
  --zone=europe-west4-a \
  --gcs-bucket=gs://oxeo-dataproc-custom-image \
  --disk-size=30
```
