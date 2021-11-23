# Secrets
To get the secrets out of the Dockerfile/containing environment, they have been placed in Google Secret Manager, GitHub Secrets and Prefect Secrets.

### Build-time secrets
These are [injected from Google Secret Manager](https://cloud.google.com/build/docs/securing-builds/use-secrets), via the [cloudbuild.yaml](./cloudbuild.yaml) config. Relies on some hacky `sed`ing to get the SSH keys to work.
Also need to add `Secret Manager Secret Accessor` role to the Cloud Build service account (e.g. `1234567@cloudbuild.gserviceaccount.com`).
- SSH token authorised on Oxeo GitHub account: to download GitHub dependencies
- Prefect API key: to connect to Prefect Cloud
- Google Service Account JSON token: to use the Google SDK within the image

### Run-time secrets
The same as above, in addition the following, stored on Prefect Secrets.
- GitHub Personal Access Token linked to Oxeo GitHub account: for Prefect to access the Flows from GitHub Storage

### Prefect Registration secrets
Stored on GitHub Secrets, used in the GitHub Action that registers updated Flows with Prefect Cloud.
- Same SSH token: download GitHub dependencies to install requirements before registration
- Prefect API key: to register the Flows with Prefect Cloud
