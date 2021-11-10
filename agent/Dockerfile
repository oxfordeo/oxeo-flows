# syntax=docker/dockerfile:1

FROM gcr.io/oxeo-main/oxeo-flows

# TODO: Get the API key from somewhere else!!
ENV GOOGLE_APPLICATION_CREDENTIALS token.json
ENV PREFECT__CLOUD__API_KEY FbXiDK-x0bAJCeU93Rwa6g

RUN prefect backend cloud

CMD ["prefect", "agent", "vertex", "start", \
  "--project=oxeo-main", \
  "--region-name=europe-west4", \
  "--service-account=prefect@oxeo-main.iam.gserviceaccount.com", \
  "--label=vertex"]
