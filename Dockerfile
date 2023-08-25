FROM openjdk:17.0
EXPOSE 8000

COPY build/libs/*.jar ./
CMD java -jar *.jar
#WORKDIR /app

#COPY target/*.jar /app/
#CMD java -jar *.jar

