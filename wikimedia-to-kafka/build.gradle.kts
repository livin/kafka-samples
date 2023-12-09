plugins {
    id("java")
    id("application")
}

group = "com.airlivin.learn.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":kafka-properties"))

    implementation("org.apache.kafka:kafka-clients:3.6.0")

    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")

    implementation("io.projectreactor:reactor-core:3.6.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass = "com.airlivin.learn.kafka.wikimedia.WikimediaProducerApp"
}

tasks.test {
    useJUnitPlatform()
}