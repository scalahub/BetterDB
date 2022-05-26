ThisBuild / organization := "io.github.scalahub"
ThisBuild / organizationName := "scalahub"
ThisBuild / organizationHomepage := Some(url("https://github.com/scalahub"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/scalahub/ScalaDB"),
    "scm:git@github.scalahub/ScalaDB.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "scalahub",
    name = "scalahub",
    email = "23208922+scalahub@users.noreply.github.com",
    url = url("https://github.com/scalahub")
  )
)

ThisBuild / description := "A relational database library in Scala for H2"
ThisBuild / licenses := List(
  "The Unlicense" -> new URL("https://unlicense.org/")
)
ThisBuild / homepage := Some(url("https://github.com/scalahub/ScalaDB"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  Some("snapshots" at nexus + "content/repositories/snapshots")
}

ThisBuild / publishMavenStyle := true

ThisBuild / versionScheme := Some("early-semver")
