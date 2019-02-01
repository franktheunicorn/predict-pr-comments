// give the user a nice default project!

// import ScalaPB
import com.trueaccord.scalapb.compiler.Version.scalapbVersion

val sparkVersion = "2.4.0"
lazy val root = (project in file(".")).



  settings(
    inThisBuild(List(
      organization := "com.holdenkarau.predict.pr.comments",
      scalaVersion := "2.11.8"
    )),
    name := "sparkProject",
    version := "0.0.1",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "com.softwaremill.sttp" %% "core" % "1.5.4",
      "com.softwaremill.sttp" %% "async-http-client-backend-future" % "1.5.4",
      "com.github.marklister" %% "product-collections" % "1.4.5",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % "test",

      "com.trueaccord.scalapb" %% "scalapb-runtime"      % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf",
      // for gRPC
      "io.grpc"                %  "grpc-netty"           % "1.4.0",
      "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
    ),


    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("io.netty.versions.properties") => MergeStrategy.first
        // Travis is giving a weird error on netty I don't see locally :(
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", "native", xs @ _*) => MergeStrategy.deduplicate
      case PathList("META-INF", xs @ _ *) => MergeStrategy.discard
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
      case PathList("org", "apache", xs @ _*) => MergeStrategy.first
      case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
      case "about.html"  => MergeStrategy.rename
      case "reference.conf" => MergeStrategy.concat
      case m =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(m)
    },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { x => false },

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
