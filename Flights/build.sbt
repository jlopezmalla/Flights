name := "Flights"

version in ThisBuild := "1.0"

scalaVersion  in ThisBuild := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

javacOptions in ThisBuild ++= Seq("-source", "1.7", "-target", "1.7")

addCommandAlias("sanity", ";clean ;compile ;scalastyle ;scoverage-all ;assembly")

libraryDependencies in ThisBuild ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "org.apache.spark" %% "spark-core" % "1.2.0" % "compile,test,provided",
  "org.apache.spark" %% "spark-sql" % "1.2.0" % "compile,test,provided",
  "org.apache.spark" %% "spark-mllib" % "1.2.0" % "compile,test,provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "compile,test" exclude("javax.servlet", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)


