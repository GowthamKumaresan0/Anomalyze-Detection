name := "KafkaLogger"

version := "0.1"

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "com.typesafe.akka" %% "akka-http" % "10.2.10",
  "com.typesafe.akka" %% "akka-stream" % "2.6.20",
  "ch.megard" %% "akka-http-cors" % "1.1.3",
  "io.spray" %% "spray-json" % "1.3.6"// <-- Added spray-json
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0"

