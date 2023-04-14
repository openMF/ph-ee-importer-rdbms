FROM openjdk:17-bullseye
EXPOSE 8000
WORKDIR /app

RUN apt update && apt install -y postgresql-client less vim && apt clean

COPY target/*.jar /app/
CMD java --enable-preview -jar *.jar

