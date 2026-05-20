plugins {
    kotlin("jvm") version "2.2.0"
    application
}

val kafkaVersion        = "3.7.0"
val duckdbVersion       = "1.5.1.0"
val jacksonVersion      = "2.17.0"
val coroutinesVersion   = "1.8.0"
val micrometerVersion   = "1.12.0"

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // DuckDB JDBC
    implementation("org.duckdb:duckdb_jdbc:$duckdbVersion")

    // Jackson YAML + JSON (jackson-dataformat-yaml brings in jackson-core/databind transitively)
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")

    // Micrometer + Prometheus  (artifact changed to micrometer-registry-prometheus in 1.10+)
    implementation("io.micrometer:micrometer-registry-prometheus:$micrometerVersion")

    // SLF4J + Logback
    implementation("ch.qos.logback:logback-classic:1.4.14")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
}

application {
    mainClass.set("io.company.duckling.MainKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "io.company.duckling.MainKt"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }) {
        exclude("META-INF/*.RSA", "META-INF/*.SF", "META-INF/*.DSA")
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

kotlin {
    jvmToolchain(21)
}
