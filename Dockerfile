FROM amazoncorretto:24.0.2-alpine3.22

COPY target/dictionaryetl-*.jar /dictionaryetl.jar

ENV TZ="US/Eastern"

ENTRYPOINT java -Xmx8192m -jar /dictionaryetl.jar