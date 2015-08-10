name := "One Tag Model"

version := "1.0"

scalaVersion := "2.10.4"

ibraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core" % "1.3.1",
  "org.apache.spark" % "spark-mllib" % "1.3.1"
  )