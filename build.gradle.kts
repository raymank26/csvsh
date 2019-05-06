plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.11")

    application

    antlr
}

repositories {
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    testImplementation("org.jetbrains.kotlin:kotlin-test")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")

    antlr("org.antlr:antlr4:4.7.2") // use ANTLR version 4
}

application {
    mainClassName = "com.github.raymank26.AppKt"
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor", "-long-messages")
}
