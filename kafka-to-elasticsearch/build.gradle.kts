fun maven(action: MavenArtifactRepository.() -> Unit) {

}

plugins {
    id("java")
    id("application")
}

group = "com.airlivin.learn.kafka"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    maven(url="https://snapshots.elastic.co/maven/")
}


dependencies {
    implementation(project(":kafka-properties"))

    implementation("org.apache.kafka:kafka-clients:3.6.0")

    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")

//    implementation("org.elasticsearch:elasticsearch:7.17.1")
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.10.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("--enable-preview")
}

tasks.withType<JavaExec> {
    jvmArgs("--enable-preview")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
}

application {
    mainClass = "com.airlivin.learn.kafka.elastic.KafkaToElasticApp"
}
