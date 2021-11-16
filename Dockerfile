# syntax=docker/dockerfile:1

FROM python:3.9.7-slim-buster
ENV PYTHONUNBUFFERED=1

ARG PREFECT
ENV PREFECT__CLOUD__API_KEY $PREFECT
COPY key /root/.ssh/id_rsa
COPY token token.json
ENV GOOGLE_APPLICATION_CREDENTIALS token.json
ENV TZ=UTC

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
RUN sed -i -e "s/-----BEGIN OPENSSH PRIVATE KEY-----/&\n/"\
      -e "s/-----END OPENSSH PRIVATE KEY-----/\n&/"\
      -e "s/\S\{70\}/&\n/g"\
      /root/.ssh/id_rsa && \
    chmod 400 /root/.ssh/id_rsa && \
    eval $(ssh-agent) && \
    ssh-add /root/.ssh/id_rsa && \
    ssh-keyscan -H github.com >> /etc/ssh/ssh_known_hosts

# Install requirements
COPY . oxeo-flows
RUN pip install --upgrade pip && \
    pip install oxeo-flows/

# This will be ignore by VertexRun instances
CMD ["prefect", "agent", "vertex", "start", \
     "--project=oxeo-main", \
     "--region-name=europe-west4", \
     "--service-account=prefect@oxeo-main.iam.gserviceaccount.com", \
     "--label=vertex"]
