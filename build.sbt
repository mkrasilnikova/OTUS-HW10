name := "homework10"

version := "0.1"


scalaVersion := "2.12.12"

lazy val sparkVersion = "3.1.2"
lazy val kafkaVersion = "2.7.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion %"provided" ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.postgresql"   % "postgresql"    % "42.2.2" ,

)
