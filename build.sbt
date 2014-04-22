organization := "me.lessis"

name := "stretchy-pants"

version := "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "net.databinder.dispatch" %% "dispatch-json4s-native" % "0.11.0",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test")

crossScalaVersions := Seq("2.10.4")

scalaVersion := crossScalaVersions.value.head

scalacOptions += "-feature"

initialCommands in console := "import stretchypants._, scala.concurrent.ExecutionContext.Implicits.global, dispatch._, org.json4s.JValue, org.json4s.JsonDSL._; val http = new Http;val es = Client(http = http);import org.json4s.native.JsonMethods.{ compact, render };def str(js: JValue) = compact(render(js))"

