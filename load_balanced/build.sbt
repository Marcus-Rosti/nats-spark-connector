import sbt.Def.settings

name := "nats-spark-connector"
organization := "io.nats"
version := "2.0.0-SNAPSHOT"
scalaVersion := "2.12.19"

val sparkVersion = "3.4.0"
val natsVersion = "2.17.0"

libraryDependencies ++= Seq(
  "io.nats" % "jnats" % natsVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)

assemblyMergeStrategy := (_ => MergeStrategy.first)
