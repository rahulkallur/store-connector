FROM maven:3.8.4 AS build
RUN mvn clean install
RUN unzip kafka-bigtable-*-distribution.zip -d kafka-bigtable
FROM openjdk:21
#RUN apk add --update tcpdump
COPY target/kafka-bigtable/kafka-bigtable-* /kafka-bigtable
WORKDIR /kafka-bigtable
RUN mv kafka-*.jar kafka.jar
ENTRYPOINT ["java", "-Xms1g", "-Xmx2g", "-jar", "kafka.jar"]