import SonatypeKeys._

organization := "com.nicta"

name := "scoobi-compatibility-cdh3"

scalaVersion := "2.10.4"

version := "1.0.2"

libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-core"   % "0.20.2-cdh3u1",
                            "org.apache.avro"   % "avro-mapred"   % "1.7.4")

resolvers ++= Seq("cloudera" at "https://repository.cloudera.com/content/repositories/releases")

publishTo <<= version { v: String =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else                             Some("staging"   at nexus + "service/local/staging/deploy/maven2")
  }

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
    <url>http://nicta.github.io/scoobi</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.opensource.org/licenses/Apache-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>http://github.com/NICTA/scoobi</url>
        <connection>scm:http:http://NICTA@github.com/NICTA/scoobi.git</connection>
      </scm>
      <developers>
        <developer>
          <id>etorreborre</id>
          <name>Eric Torreborre</name>
          <url>http://etorreborre.blogspot.com/</url>
        </developer>
      </developers>
    )

credentials := Seq(Credentials(Path.userHome / ".sbt" / "scoobi.credentials"))

sonatypeSettings
