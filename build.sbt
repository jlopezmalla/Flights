name := "Flights"

version in ThisBuild := "0.1.0"

organization in ThisBuild := "com.stratio"

scalaVersion in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

addCommandAlias("sanity", ";clean ;compile ;scalastyle ;coverage ;test ;assembly")

val sparkVersion = "1.5.2"

libraryDependencies in ThisBuild ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "2.0.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile,test,provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile,test,provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "compile,test,provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile,test,provided",
  "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.1_0.2.1" % "compile,test",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "compile, test" exclude("javax.servlet", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)
