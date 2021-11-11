# syntax=docker/dockerfile:1

FROM python:3.9.7-slim-buster

# Instructions from https://cloud.google.com/sdk/docs/install
RUN apt-get update && \
  apt-get install -y \
    openssh-client git apt-transport-https ca-certificates gnupg curl && \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
  apt-get update && \
  apt-get install -y google-cloud-sdk

# Set up GitHub SSH key
RUN mkdir /root/.ssh
RUN gcloud config set account 292453623103@cloudbuild.gserviceaccount.com && \
  gcloud secrets versions access latest \
    --project=oxeo-main \
    --secret=chris-github-pat > /root/.ssh/id_rsa && \
  chmod 400 /root/.ssh/id_rsa
RUN eval $(ssh-agent) && \
    ssh-add /root/.ssh/id_rsa && \
    ssh-keyscan -H github.com >> /etc/ssh/ssh_known_hosts

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
  pip install -r requirements.txt

# TODO: Get the API key from somewhere else!!
ENV PREFECT__CLOUD__API_KEY FbXiDK-x0bAJCeU93Rwa6g

# This will be ignore by VertexRun instances
CMD ["prefect", "agent", "vertex", "start", \
  "--project=oxeo-main", \
  "--region-name=europe-west4", \
  "--service-account=prefect@oxeo-main.iam.gserviceaccount.com", \
  "--label=vertex"]
