#!/bin/bash

AWS_REGION="ap-south-1"
ECR_REPO="905418388706.dkr.ecr.ap-south-1.amazonaws.com"

ECR_LOGIN_CMD=$(aws ecr get-login-password --region $AWS_REGION)
echo "$ECR_LOGIN_CMD" | docker login --username AWS --password-stdin $ECR_REPO

docker build -f dockerfiles/ECSRunTaskDockerfile -t batch_processing_task .

docker tag batch_processing_task:latest $ECR_REPO/batch_processing_task:latest

docker push $ECR_REPO/batch_processing_task:latest
