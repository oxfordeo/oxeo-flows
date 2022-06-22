# AWS EKS setup

## Basic setup
Some stuff based on [this Prefect article](https://towardsdatascience.com/distributed-data-pipelines-made-easy-with-aws-eks-and-prefect-106984923b30).

Check you're logged in with the right user:
```bash
aws sts get-caller-identity
```

Create a repository if needed:
```bash
aws ecr create-repository --repository-name flows
```

Log Docker in if needed:
```bash
aws ecr get-login-password --region eu-central-1 | \
  docker login --username AWS --password-stdin 413730540186.dkr.ecr.eu-central-1.amazonaws.com
```

Create a cluster (this takes long):
```bash
eksctl create cluster --name oxeo-eks --region eu-central-1 --fargate
```

Load the config into kubectl:
```bash
aws eks update-kubeconfig --region eu-central-1 --name oxeo-eks
```

Check that it's loaded:
```bash
kubectl config current-context
```

## Roles
1. Having made a role `eksClusterRole` with all the EKS permissions, grant role assume permissions on `oxeo-dev` users, and trust relationships on eksClusterRole.
2. Add the role to the aws-auth:
```bash
eksctl create iamidentitymapping --cluster oxeo-eks \
  --arn arn:aws:iam::413730540186:role/eksClusterRole \
  --username admin \
  --group system:masters
```
3. Update the `kubeconfig` to assume the role:
```bash
aws eks --region eu-central-1 update-kubeconfig --name oxeo-eks \
  --role-arn "arn:aws:iam::413730540186:role/eksClusterRole"
```
Any oxeo-dev users can then use the same role to get into any cluster.

Load the config for a cluster using another role:
```bash
aws eks --region eu-central-1 update-kubeconfig \
  --name oxeo-eks \
  --role-arn "arn:aws:iam::413730540186:role/eksClusterRole"
```
