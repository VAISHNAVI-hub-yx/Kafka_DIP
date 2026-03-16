name := "Kafka"

version := "1.0"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
"org.apache.kafka" % "kafka-clients" % "3.6.1",
"org.apache.hadoop" % "hadoop-common" % "3.3.6",
"org.slf4j" % "slf4j-simple" % "2.0.9"
)