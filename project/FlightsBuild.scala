import org.scalastyle.sbt._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList, _}
import sbtunidoc.Plugin._
import scoverage.ScoverageSbtPlugin._
import scoverage._

object FlightsBuild extends Build {


  val flightsMergeStrategy = mergeStrategy in assembly := {
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf/services/.*") => MergeStrategy.concat
    case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
    case _ => MergeStrategy.first
  }

  lazy val testParallelSettings = Seq(parallelExecution in ScoverageTest := false, parallelExecution in Test := false)

  lazy val flightsAssembly = assemblySettings ++ flightsMergeStrategy
  lazy val AcceptanceTest = config("acceptance") extend(Test)

  lazy val main = (Project(id = "flights", base = file("."))
    settings(unidocSettings: _*)
    settings(ScoverageSbtPlugin.instrumentSettings: _*)
    settings(ScalastylePlugin.Settings: _*)
    configs(IntegrationTest)
    settings(flightsAssembly: _*)
    settings(Defaults.itSettings: _*)
    settings(testParallelSettings: _*)
    settings(aggregate in test := false)
    )
}
