# Building Docker images for AWS

From repo root:
```bash
DOCKER_BUILDKIT=1 docker build \
  -t 413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows \
  -f infra/Dockerfile \
  --ssh=default .
```

Then push:
```bash
docker push 413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows
```
