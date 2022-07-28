name := "akka-websockets-spark-cassandra"
version := "0.1"
scalaVersion := "2.12.10"


val akkaVersion = "2.5.32"
val akkaHttpVersion = "10.1.15"
val sparkVersion = "3.3.0"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided,

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
//  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-jackson" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.kafka" % "kafka-streams" % kafkaVersion


)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0" % Provided,
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0"  % Provided,
    "com.fasterxml.jackson.core" % "jackson-core" % "2.13.0"  % Provided
  )
}

excludeDependencies ++= {
  Seq(
    ExclusionRule("org.apache.hadoop", "hadoop-client-api"),
    ExclusionRule("org.apache.hadoop", "hadoop-client-runtime")
  )
}
