# syntax=docker/dockerfile:1

FROM python:3.9.7-slim-buster

# Instructions from https://cloud.google.com/sdk/docs/install
COPY secrets/oxeo-main-prefect.json token.json
RUN apt-get update && \
  apt-get install -y \
    openssh-client git apt-transport-https ca-certificates gnupg curl && \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
  apt-get update && \
  apt-get install -y google-cloud-sdk && \
  gcloud auth activate-service-account \
  --key-file=token.json

# Set up GitHub SSH key
RUN mkdir /root/.ssh
COPY secrets/deploy_key /root/.ssh/id_rsa
RUN eval $(ssh-agent) && \
    ssh-add /root/.ssh/id_rsa && \
    ssh-keyscan -H github.com >> /etc/ssh/ssh_known_hosts

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
  pip install -r requirements.txt

# TODO: Get the API key from somewhere else!!
ENV GOOGLE_APPLICATION_CREDENTIALS token.json
ENV PREFECT__CLOUD__API_KEY FbXiDK-x0bAJCeU93Rwa6g

# This will be ignore by DockerRun instances
CMD ["prefect", "agent", "vertex", "start", \
  "--project=oxeo-main", \
  "--region-name=europe-west4", \
  "--service-account=prefect@oxeo-main.iam.gserviceaccount.com", \
  "--label=vertex"]
