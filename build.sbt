name := "ScalaDB"

version := "0.1"

scalaVersion := "2.12.8"

lazy val ScalaUtils = RootProject(uri("https://github.com/scalahub/ScalaUtils.git"))
//lazy val ScalaUtils = RootProject(uri("../ScalaUtils"))

lazy val root = project in file(".") dependsOn ScalaUtils

// https://mvnrepository.com/artifact/net.snaq/dbpool
libraryDependencies += "net.snaq" % "dbpool" % "7.0.1"

// https://mvnrepository.com/artifact/org.bouncycastle/bcprov-jdk15on
libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.61"

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.199"

initialize := {
    /** Java specification version compatibility rule. */
    object CompatibleJavaVersion extends VersionNumberCompatibility {
      def name = "Java specification compatibility"
      def isCompatible(current: VersionNumber, required: VersionNumber) =
	current.numbers.zip(required.numbers).foldRight(required.numbers.size<=current.numbers.size)((a,b) => (a._1 > a._2) || (a._1==a._2 && b))
      def apply(current: VersionNumber, required: VersionNumber) = isCompatible(current, required)
    }
    val _ = initialize.value // run the previous initialization
    val required = VersionNumber("1.8") 
    val curr = VersionNumber(sys.props("java.specification.version"))
    assert(CompatibleJavaVersion(curr, required), s"Java $required or above required. Currently $curr")
}

