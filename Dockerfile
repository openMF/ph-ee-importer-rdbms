FROM openjdk:17.0
EXPOSE 8000

WORKDIR /app

COPY target/*.jar /app/
CMD java -jar *.jar

