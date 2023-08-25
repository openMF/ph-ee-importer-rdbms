FROM openjdk:17
EXPOSE 8000

COPY build/libs/*.jar .
CMD java --enable-preview -jar *.jar

