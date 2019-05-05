name := "BetterDB"

version := "0.1"

scalaVersion := "2.12.8"

lazy val commonUtilGitRepo = "git:https://github.com/scalahub/CommonUtil.git/#master"

lazy val commonUtil = RootProject(uri(commonUtilGitRepo))

lazy val root = project in file(".") dependsOn commonUtil

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.199" % Test

// https://mvnrepository.com/artifact/net.snaq/dbpool
libraryDependencies += "net.snaq" % "dbpool" % "7.0.1"

// https://mvnrepository.com/artifact/org.bouncycastle/bcprov-jdk15on
libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.61"

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.199" 
