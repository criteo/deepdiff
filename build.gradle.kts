allprojects {
    repositories {
        // Use jcenter for resolving dependencies.
        // You can declare any Maven/Ivy/file repository here.
        jcenter()
    }
    val shortScalaVersion = "2.11"
    val sparkVersion = "[2.4.0, 3.0.0["
    val libraries: Map<String, String> by extra(mapOf(
            "org.scala-lang:scala-library" to "org.scala-lang:scala-library:$shortScalaVersion.+",
            "org.apache.spark:spark-core" to "org.apache.spark:spark-core_$shortScalaVersion:$sparkVersion",
            "org.apache.spark:spark-sql" to "org.apache.spark:spark-sql_$shortScalaVersion:$sparkVersion",
            "com.typesafe:config" to "com.typesafe:config:[1.3, 2.0[",

            // test dependencies
            "org.scalatest:scalatest" to "org.scalatest:scalatest_$shortScalaVersion:3.2.2",
            "com.vladsch.flexmark:flexmark-all" to "com.vladsch.flexmark:flexmark-all:0.36.8"
    ))
}
