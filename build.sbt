name := "ScalaDB"

scalaVersion := "2.12.10"

//resolvers += "SonaType Snapshots s01" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"
resolvers += "SonaType" at "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"

libraryDependencies += "io.github.scalahub" %% "scalautils" % "1.0"

libraryDependencies += "net.snaq" % "dbpool" % "7.0.1"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.61"

libraryDependencies += "com.h2database" % "h2" % "1.4.199"

initialize := {

  /** Java specification version compatibility rule. */
  object CompatibleJavaVersion extends VersionNumberCompatibility {
    def name = "Java specification compatibility"
    def isCompatible(current: VersionNumber, required: VersionNumber) =
      current.numbers
        .zip(required.numbers)
        .foldRight(required.numbers.size <= current.numbers.size)((a, b) =>
          (a._1 > a._2) || (a._1 == a._2 && b)
        )
    def apply(current: VersionNumber, required: VersionNumber) =
      isCompatible(current, required)
  }
  val _ = initialize.value // run the previous initialization
  val required = VersionNumber("1.8")
  val curr = VersionNumber(sys.props("java.specification.version"))
  assert(
    CompatibleJavaVersion(curr, required),
    s"Java $required or above required. Currently $curr"
  )
}
