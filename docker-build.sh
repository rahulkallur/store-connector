#!/bin/sh


mvn clean install
unzip target/kafka-bigtable-*-distribution.zip -d target/kafka-bigtable


# build docker image
docker build -t zeliotcentralregistry/kafka-bigtable:1.0.7 .

# push the docker image to Google Container Registry
docker push zeliotcentralregistry/kafka-bigtable:1.0.7

