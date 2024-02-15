#!/usr/bin/env bash

set -e

aws-vault exec daily-engineer -- aws ecr get-login-password \
     --region us-west-2 | docker login \
     --username AWS --password-stdin \
     557238627320.dkr.ecr.us-west-2.amazonaws.com
mvn clean package
docker build -t kafka-connect-elasticsearch-source --platform linux/amd64 .
docker tag kafka-connect-elasticsearch-source:latest 557238627320.dkr.ecr.us-west-2.amazonaws.com/kafka-connect-elasticsearch-source:latest
docker push 557238627320.dkr.ecr.us-west-2.amazonaws.com/kafka-connect-elasticsearch-source:latest
