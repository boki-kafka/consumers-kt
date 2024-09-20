plugins {
    kotlin("jvm") version "2.0.10"
}

group = "com.boki"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("io.github.oshai:kotlin-logging:7.0.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
    implementation("net.datafaker:datafaker:2.3.1")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}
