FROM eclipse-temurin:21-jdk
EXPOSE 8080

COPY build/libs/*.jar ./
CMD java -jar *.jar
