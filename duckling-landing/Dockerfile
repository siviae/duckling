FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY build/libs/duckling-landing-*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--config", "duckling.yaml"]
