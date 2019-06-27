plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.11")

    application

    distribution

    antlr
}

repositories {
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("org.apache.commons:commons-csv:1.6")

    implementation("com.google.guava:guava:28.0-jre")

    implementation("org.lmdbjava:lmdbjava:0.7.0")

    implementation("com.jakewharton.fliptables:fliptables:1.0.2")

    implementation("org.slf4j:slf4j-api:1.7.26")

    implementation("org.jline:jline:3.11.0")

    implementation("org.fusesource.jansi:jansi:1.18")

    testImplementation("org.jetbrains.kotlin:kotlin-test")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")

    testImplementation("io.mockk:mockk:1.9.3")

    antlr("org.antlr:antlr4:4.7.2")
}

application {
    mainClassName = "com.github.raymank26.AppKt"
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor", "-long-messages", "-package", "com.github.raymank26.sql")
}

//val compileKotlin: KotlinCompile by tasks
project.tasks.getByName("compileKotlin").dependsOn(project.tasks.getByName("generateGrammarSource"))

