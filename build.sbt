name := "sangria-rxscala"
organization := "org.sangria-graphql"
version := "1.0.1-SNAPSHOT"

description := "Sangria RxScala integration"
homepage := Some(url("http://sangria-graphql.org"))
licenses := Seq("Apache License, ASL Version 2.0" → url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion := "2.12.1"
crossScalaVersions := Seq("2.11.8", "2.12.1")

scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions ++= {
  if (scalaVersion.value startsWith "2.12")
    Seq.empty
  else
    Seq("-target:jvm-1.7")
}

libraryDependencies ++= Seq(
  "org.sangria-graphql" %% "sangria-streaming-api" % "1.0.0",
  "io.reactivex" %% "rxscala" % "0.26.5",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

git.remoteRepo := "git@github.com:sangria-graphql/sangria-rxscala.git"

// Publishing

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := (_ ⇒ false)
publishTo := Some(
  if (version.value.trim.endsWith("SNAPSHOT"))
    "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  else
    "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")

resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

// Site and docs

site.settings
site.includeScaladoc()
ghpages.settings

// nice *magenta* prompt!

shellPrompt in ThisBuild := { state ⇒
  scala.Console.MAGENTA + Project.extract(state).currentRef.project + "> " + scala.Console.RESET
}

// Additional meta-info

startYear := Some(2016)
organizationHomepage := Some(url("https://github.com/sangria-graphql"))
developers := Developer("OlegIlyenko", "Oleg Ilyenko", "", url("https://github.com/OlegIlyenko")) :: Nil
scmInfo := Some(ScmInfo(
  browseUrl = url("https://github.com/sangria-graphql/sangria-rxscala.git"),
  connection = "scm:git:git@github.com:sangria-graphql/sangria-rxscala.git"
))