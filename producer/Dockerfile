# Use Maven + JDK image to build
FROM maven:3.8.1-openjdk-17-slim AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Use slim JDK to run the app
FROM openjdk:17-jdk-slim
WORKDIR /app
COPY --from=build /app/target/producer-0.0.1-SNAPSHOT.jar app.jar

CMD ["java", "-jar", "app.jar"]