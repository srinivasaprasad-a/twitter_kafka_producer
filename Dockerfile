FROM openjdk:8u151-jre-alpine
MAINTAINER SrinivasaPrasadA

WORKDIR /opt/
RUN pwd

RUN apk add --no-cache bash jq
RUN apk update
RUN apk add openssh

ADD target/twitterkafkaproducer.jar .

CMD java -jar twitterkafkaproducer.jar
