plugins {
    kotlin("jvm") version "2.2.0" apply false
}

allprojects {
    group = "io.company.duckling"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}
