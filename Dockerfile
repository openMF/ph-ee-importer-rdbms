FROM openjdk:13
EXPOSE 8000

COPY target/importer-*.jar .
CMD java -jar *.jar

