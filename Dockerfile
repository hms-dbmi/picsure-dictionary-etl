FROM maven:3-amazoncorretto-21 AS build

COPY pom.xml .

RUN mvn -B dependency:go-offline

COPY src src

RUN mvn -B package -DskipTests

FROM amazoncorretto:21.0.1-alpine3.18

COPY --from=build target/dictionaryetl-*.jar /dictionaryetl.jar

ENV TZ="US/Eastern"

ENTRYPOINT java -Xmx8192m -jar /dictionaryetl.jar