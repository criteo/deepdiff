plugins {
    scala
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    id("com.github.maiflai.scalatest") version "0.26"
}

dependencies {
    val libraries: Map<String, String> by rootProject.extra
    implementation(libraries.getValue("org.scala-lang:scala-library"))
    api(libraries.getValue("com.typesafe:config"))

    testImplementation(libraries.getValue("org.scalatest:scalatest"))
    testRuntimeOnly(libraries.getValue("com.vladsch.flexmark:flexmark-all"))
}

tasks {
    test {
        maxParallelForks = 1
        testLogging {
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showExceptions = true
        }
    }
}