<img src="oxeo_logo.png" alt="oxeo logo" width="600"/>

[OxEO](https://www.oxfordeo.com/) is an earth observation water risk company. This repository builds and deploys OxEO's data pipeline Flows via Prefect. OxEO's data service is comprised of three repos: [oxeo-flows](https://github.com/oxfordeo/oxeo-flows), [oxeo-water](https://github.com/oxfordeo/oxeo-water), and [oxeo-api](https://github.com/oxfordeo/oxeo-api). This work was generously supported by the [European Space Agency Φ-lab](https://philab.esa.int/) and [World Food Programme (WFP) Innovation Accelerator](https://innovation.wfp.org/) as part of the [EO & AI for SDGs Innovation Initiative](https://wfpinnovation.medium.com/how-can-earth-observation-and-artificial-intelligence-help-people-in-need-5e56efc5c061).

Copyright © 2022 Oxford Earth Observation Ltd.

---

# oxeo-flows
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![prefect-register](https://github.com/oxfordeo/oxeo-flows/actions/workflows/prefect-register.yml/badge.svg)](https://github.com/oxfordeo/oxeo-flows/actions/workflows/prefect-register.yml)
[![build-images](https://github.com/oxfordeo/oxeo-flows/actions/workflows/build-images.yml/badge.svg)](https://github.com/oxfordeo/oxeo-flows/actions/workflows/build-images.yml)

![Infrastructure diagram](./diagram.svg)

## Getting started
Currently any internal dependencies (such as the `oxeo/flows/utils.py` file) can *not* be pulled from GitHub, and therefore must be installed into the Docker image.
Have set the GitHub Action to watch `utils.py` only. Not sure what the best method is...

### Project structure
Currently quite basic. At some point we'll probably want to separate actual business logic, from task definitions, from Flow definitions.
Something like what is described [here](https://github.com/PrefectHQ/prefect/issues/1300).

```
.
├── cloudbuild.yaml      # insert secrets into Cloud Build
├── setup.cfg            # Requirements and things in here
├── Dockerfile
├── oxeo
│   └── flows            # Flow logic in here!
│       ├── extract.py
│       ├── __init__.py  # Constants, image names etc here
│       ├── predict.py
│       ├── template.py  # TEMPLATE FLOW FILE HAVE A LOOK!
│       └── utils.py
├── infra                # Docker, Kubernetes and Dask stuff
│   └── ...
├── docs                 # useful docs and commands
│   └── ...
└── ...
```

### Installation
Install the dependencies and the library in a virtualenv:
```
pip install -e .[dev]
pre-commit install
```

### Prefect Cloud
Log in at [Prefect Cloud](https://cloud.prefect.io/) and get an API key from the user account page (different from an agent/project key).

Tell core that we're working with Cloud and authenticate:
```
prefect backend cloud
prefect auth login --key <your-api-key>
```
(If you get authentication errors, you may need to delete `~/.prefect/`.)

## Adding a flow
1. Make a copy of the [template](./oxeo/flows/template.py) as a starting point.
2. Have a look through the comments there!
3. Write some tasks and link them up into a flow!
4. Add any new dependencies to requirements.txt

### Run a flow locally
This works even if the Flow is set up with `KubernetesRun` etc
(i.e. the `storage` and `run_config` arguments you passed to your `Flow` will be ignored, but the `executor` won't!).

Probably best to try with the plain `DaskExecutor` to start off with.
```
prefect run -p oxeo/flows/extract.py \
  --param aoi=aoi.geojson \
  --param credentials=token.json
```

### Register a flow
```
prefect register --project <proj-name> -p oxeo/flows/extract.py
```

### Start the agent to listen for jobs
```
prefect agent local start --label=<your-name>
```

Then you can go to the UI, start a new run, and **change the labels** in the parameters to *only* match the label you use above.
Your local agent should pick up the Flow run from Prefect Cloud and run it, while sending logs back to the Cloud.

### Control
And control from the [web UI](https://cloud.prefect.io/)!

## CI/CD
On push to GitHub, the following will happen, only running when needed (by specifying which files to watch for each):
### GitHub Actions
- Register/update all Flows with Prefect Cloud
- Build the Docker image
- Build a Packer image based on the new Docker image

### Cloud Run
Cloud Run is set to [continuously deploy](https://cloud.google.com/run/docs/continuous-deployment-with-cloud-build) the `sat-extractor` image and service from the [oxfordeo fork](https://github.com/oxfordeo/sat-extractor). Did this by first building once using `sat-extractor` CLI (to create the PubSub resources), and *then* add the continuous deployment. (Nothing to do with this repo, just a note!)

# More docs
In [docs](docs/).
