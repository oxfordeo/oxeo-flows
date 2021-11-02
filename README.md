# oxeo-cloud

Forked from: https://github.com/GoogleCloudPlatform/buildpack-samples/tree/master/sample-python

## Local dev
```
pip install -r requirements.txt
FLASK_DEBUG=True FLASK_APP=web.py flask run -p 8000
```

##
Test
```
curl -X POST http://127.0.0.1:8000/api/ \
  -H 'Content-Type: application/json' \
  -d '{"message": {"data": {"msg": "Hello!"}}}'
```

## Buildpack
Install Buildpack:
```
sudo add-apt-repository ppa:cncf-buildpacks/pack-cli
sudo apt install pack-cli
```

Run Locally with Buildpacks & Docker:
```
pack build --builder=gcr.io/buildpacks/builder sample-python
docker run -it -ePORT=8080 -p8080:8080 chris-test
```

## Coudrun
[![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run)

Run the following (can run again, will automatically overwrite previous version):
```
gcloud run deploy chris-test --source=. \
  --region=europe-west4 --no-allow-unauthenticated \
  --memory=4G --timeout=15m --platform=managed \
  --concurrency=1
```

Add service worker permission to launch from PubSub:
```
SERV_AC='sat-extractor@oxeo-main.iam.gserviceaccount.com'

gcloud run services add-iam-policy-binding chris-test \
  --role=roles/run.invoker --region=europe-west4 \
  --member=serviceAccount:$SERV_AC
```

Get endpoint:
```
GCR_URL=$(gcloud run services describe chris-test \
  --platform=managed --region=europe-west4 \
  --format='value(status.url)')
```

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
  --topic='projects/oxeo-main/topics/chris-test-dlq' \

```

Get proj number and service account:
```
PROJ_NUMBER=$(gcloud projects list \
--filter="$(gcloud config get-value project)" \
--format="value(PROJECT_NUMBER)")

PUBSUB_SERV_AC="service-$PROJ_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com"
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
  --message='{"msg": "This will apear in Cloud Run logs!"}'
  --attribute='type=model'
```

This message will fail (because the app wants JSON in the message body) and go to DLQ:
```
gcloud pubsub topics publish chris-test \
  --message="This will fail, and retry five times"
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
