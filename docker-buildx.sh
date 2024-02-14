#!/bin/sh


mvn clean install
unzip target/kafka-bigtable-*-distribution.zip -d target/kafka-bigtable


# build docker image
docker buildx build --platform linux/amd64 --tag zeliotcentralregistry/kafka-bigtable:1.0.5 --push .
