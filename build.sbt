organization := "me.lessis"

name := "stretchy-pants"

version := "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq("net.databinder.dispatch" %% "dispatch-json4s-native" % "0.11.0")

crossScalaVersions := Seq("2.10.4")

scalaVersion := crossScalaVersions.value.head

initialCommands in console := "import stretchypants._, scala.concurrent.ExecutionContext.Implicits.global, dispatch._; val http = new Http;val es = Client(http = http);"

