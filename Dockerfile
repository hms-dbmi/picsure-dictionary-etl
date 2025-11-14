FROM maven:3.9.11-amazoncorretto-24 AS build

COPY pom.xml .

RUN mvn -B dependency:go-offline

COPY src src

RUN mvn -B package -DskipTests

FROM amazoncorretto:24.0.2-alpine3.22

COPY --from=build target/dictionaryetl-*.jar /dictionaryetl.jar

ENV TZ="US/Eastern"

ENTRYPOINT java -Xmx8192m -jar /dictionaryetl.jar