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

Create a cluster (this takes long).
(But don't actually do this! Do it with Karpenter as described below.)
```bash
eksctl create cluster --name oxeo-eks --region eu-central-1
```

Load the config into kubectl:
```bash
aws eks update-kubeconfig --region eu-central-1 --name oxeo-eks
```

Check that it's loaded:
```bash
kubectl config current-context
```

## Create a cluster with Karpenter scaling
I did this using the [eksctl instructions](https://eksctl.io/usage/eksctl-karpenter/), but that's limited to Karpenter `v0.9.1` for some reason.

There are also the generic [karpenter instructions](https://karpenter.sh/v0.12.0/getting-started/getting-started-with-eksctl/) with full IAM instructions.

I didn't do the IAM stuff from those instructions, but I did do IAM stuff while I was trying to get Cluster Autoscaler to work, following [these AWS instructions](https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html#cluster-autoscaler). It's possible some of those steps are necessary! Otherwise just try the karpenter instructions from above.

```bash
eksctl create cluster -f infra/cluster.yaml
```

Then add the provisioner deployment:
```bash
kubectl apply -f infra/provisioner.yaml
```

## Roles
Having made a role `eksClusterRole` with all the EKS permissions, grant role assume permissions on `oxeo-dev` users, and trust relationships on eksClusterRole.

Add the role to the aws-auth:
```bash
eksctl create iamidentitymapping --cluster oxeo-eks \
  --arn arn:aws:iam::413730540186:role/eksClusterRole \
  --username admin \
  --group system:masters
```

Now any oxeo-dev user can load the config for a cluster by assuming the role:
```bash
aws eks --region eu-central-1 update-kubeconfig \
  --name oxeo-eks \
  --role-arn "arn:aws:iam::413730540186:role/eksClusterRole"
```

## Run the Prefect Agent
As before:
```bash
export PREFECT_KUB_KEY=...
cat infra/kubernetes-agent.yaml \
  | sed "s/API_KEY_HERE/$PREFECT_KUB_KEY/g" \
  | kubectl apply -f -
```

## Enable GPU support
```bash
kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.12.2/nvidia-device-plugin.yml
```

## Connecting to RDS from within EKS
What a pain!

Useful stuff here to setup peering between the EKS VPC and the RDS VPC: [link](https://dev.to/bensooraj/accessing-amazon-rds-from-aws-eks-2pc3#setup-the-eks-cluster).

Just set up an RDS DB and EKS cluster as normal, and then followed the instructions on setting up the peering. Make sure to use the correct CIDR details from the peering screen.


