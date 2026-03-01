FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /app

COPY imdb-jena-fuseki/pom.xml .

RUN mvn dependency:go-offline

COPY imdb-jena-fuseki/src ./src

RUN mvn clean package

FROM eclipse-temurin:25-jre-jammy

COPY --from=builder /app/target/imdb-jena-fuseki-*.jar ./app.jar

ENTRYPOINT ["java", "-jar", "/app.jar"]
