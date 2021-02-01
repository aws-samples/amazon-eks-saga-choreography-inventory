# Introduction

This project implements the `Inventory` microservice that participates in the saga pattern with Amazon Elastic Kubernetes Service (EKS).

- [Introduction](#introduction)
  - [Usage](#usage)
    - [Build](#build)
    - [Deploy](#deploy)
  - [Clean-up](#clean-up)
  - [Inputs and outputs](#inputs-and-outputs)

## Usage

### Build

1. Clone the repo.

```bash
git clone ${GIT_URL}/eks-saga-inventory
```

> Skip this step, if instructions to build images were followed in the `eks-saga-aws` repository. Else, follow the steps below to build and push the image to Amazon ECR.

2. Build the Docker image and push to Docker repository.

```bash
cd eks-saga-inventory/src

aws ecr get-login-password --region ${REGION_ID} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION_ID}.amazonaws.com
IMAGE_URI=${ACCOUNT_ID}.dkr.ecr.${REGION_ID}.amazonaws.com/eks-saga/inventory
docker build -t ${IMAGE_URI}:0.0.0 . && docker push ${IMAGE_URI}:0.0.0
```

### Deploy

1. Create `ConfigMap` running the command below from the `yaml` folder. Change `Asia/Kolkata` to appropriate timezone value in the `sed` command below. Then, run the following commands to create the `ConfigMap`.

```bash
cd eks-saga-inventory/yaml
RDS_DB_ID=eks-saga-db
DB_ENDPOINT=`aws rds describe-db-instances --db-instance-identifier ${RDS_DB_ID} --query 'DBInstances[0].Endpoint.Address' --output text`
sed -e 's#timeZone#Asia/Kolkata#g' \
  -e 's/regionId/'"${REGION_ID}"'/g' \
  -e 's/accountId/'"${ACCOUNT_ID}"'/g' \
  -e 's/dbEndpoint/'"${DB_ENDPOINT}"'/g' \
  cfgmap.yaml | kubectl -n eks-saga create -f -
```

2. Run the following command to deploy Inventory microservice.

```bash
sed -e 's/regionId/'"${REGION_ID}"'/g' \
  -e 's/accountId/'"${ACCOUNT_ID}"'/g' \
  inventory.yaml | kubectl -n eks-saga create -f -
```

## Clean-up

To remove objects for Inventory microservice, run this command.

```bash
kubectl -n eks-saga delete deployment/eks-saga-inventory configmap/eks-saga-inventory
```

## Inputs and outputs

The Inventory microservice publishes to the following AWS SNS topics.

| Topic                        | Message                                         |
| ---------------------------- | ----------------------------------------------- |
| `eks-saga-inventory-success` | Published to when order is successfully raised. |
| `eks-saga-inventory-fail`    | Published to if order is not raised.            |


The Inventory microservice subscribes to the following AWS SQS queue.

| Topic                      | Message                                |
| -------------------------- | -------------------------------------- |
| `eks-saga-inventory-input` | Subscribed to the order success topic. |
