ARG JAVA_BASE_IMAGE=maven:3.9.11-eclipse-temurin-21
FROM ${JAVA_BASE_IMAGE} AS build
ARG BUILD_PROXY=""
WORKDIR /build
COPY build-maven.sh /usr/local/bin/build-maven
RUN chmod +x /usr/local/bin/build-maven
COPY pom.xml .
RUN build-maven dependency:go-offline
COPY src ./src
RUN build-maven package
FROM maven:3.9.11-eclipse-temurin-21
WORKDIR /app
COPY --from=build /build/target/order-review-1.0.0.jar /app/app.jar
RUN mkdir -p /data
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
