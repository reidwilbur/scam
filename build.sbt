
lazy val commonSettings = Seq(
  organization := "com.wilb0t",
  version := "0.1.0",
  scalaVersion := "2.11.4"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "scam"
  )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "it,test"

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "it,test"

parallelExecution in IntegrationTest := false

scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked")

exportJars := true
