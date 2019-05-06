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
}

application {
    mainClassName = "com.github.raymank26.AppKt"
}
