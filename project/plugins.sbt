// Formats lots of files(doesn't seem to break spotless),
// also requires renaming scalafmt.conf to .scalafmt.conf and update in xml
// In short, TODO for a different PR
//addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.1")
//addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"
