FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY build/libs/duckling-landing-*.jar app.jar

# Pre-install the DuckLake extension so INSTALL ducklake at runtime is a no-op
RUN java -cp app.jar io.company.duckling.ExtensionInstallerKt

ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--config", "duckling.yaml"]
