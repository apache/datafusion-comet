import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.autoImport.PB

import scala.sys.process._

// -----------------------------------------------------------------------------
// Root / Global Settings
// -----------------------------------------------------------------------------
// Currently this needs to be modified manually, which isn't great but works for now
ThisBuild / version           := "0.6.0-SNAPSHOT"
ThisBuild / organization      := "org.apache.datafusion"
ThisBuild / Keys.scalaVersion := "2.12.17"

// Global Repositories + Dep Overrides
ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.sonatypeOssRepos("releases").head,
  Resolver.sonatypeOssRepos("snapshots").head,
  "Apache Arrow" at "https://arrow.apache.org/docs/java/"
).map(_.withAllowInsecureProtocol(true))

// Override Jackson versions
ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"    %   "jackson-databind"      % "2.15.2",
  "com.fasterxml.jackson.core"    %   "jackson-core"          % "2.15.2",
  "com.fasterxml.jackson.module"  %%  "jackson-module-scala"  % "2.15.2"
)

// -----------------------------------------------------------------------------
// Version Variables, TODO: make this configurable
// -----------------------------------------------------------------------------
val sparkVersion      = "3.5.1"
val arrowVersion      = "16.0.0"
val parquetVersion    = "1.13.1"
val scalatestVersion  = "3.2.9"
val protobufVersion   = "3.21.12"
val scalapbVersion    = "0.11.13"
val scalaVersionUsed  = "2.12.17"
val cometShadePackage = "org.apache.comet.shaded"

// -----------------------------------------------------------------------------
// Common Dependencies
// -----------------------------------------------------------------------------
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core"            % sparkVersion,
  "org.apache.spark" %% "spark-sql"             % sparkVersion,
  "org.apache.spark" %% "spark-catalyst"        % sparkVersion,
  "org.apache.spark" %% "spark-network-common"  % sparkVersion
)
  .map(_ % "provided" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.module")
  ))

val arrowDependencies = Seq(
  "org.apache.arrow" % "arrow-vector"         % arrowVersion,
  "org.apache.arrow" % "arrow-memory-core"    % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty"   % arrowVersion,
  "org.apache.arrow" % "arrow-memory-unsafe"  % arrowVersion,
  "org.apache.arrow" % "arrow-format"         % arrowVersion,
  "org.apache.arrow" % "arrow-c-data"         % arrowVersion
)

val testDependencies = Seq(
  "org.scalatest"     %%  "scalatest"     % scalatestVersion,
  "org.scalatestplus" %%  "junit-4-13"    % "3.2.9.0",
  "junit"             %   "junit"         % "4.13.2",
  "org.assertj"       %   "assertj-core"  % "3.24.2"
).map(_ % Test)

// -----------------------------------------------------------------------------
// OS / Architecture / Java Detection
// -----------------------------------------------------------------------------
val os = System.getProperty("os.name").toLowerCase
val arch = System.getProperty("os.arch").toLowerCase

val platform: String =
  if      (os.contains("linux"))  "linux"
  else if (os.contains("mac"))    "darwin"
  else sys.error(s"Unsupported OS: $os")

val archFolder: String =
  if      (arch == "amd64"    || arch == "x86_64") "amd64"
  else if (arch == "aarch64"  || arch == "arm64")  "aarch64"
  else sys.error(s"Unsupported architecture: $arch")

val rustTarget: String = {
  (platform, archFolder) match {
    case ("linux",  "amd64")           => "x86_64-unknown-linux-gnu"
    case ("linux",  "aarch64")         => "aarch64-unknown-linux-gnu"
    case ("darwin", "amd64")           => "x86_64-apple-darwin"
    case ("darwin", "aarch64")         => "aarch64-apple-darwin"
    case (otherPlatform, otherArch)    => sys.error(s"Unsupported platform/arch: $otherPlatform/$otherArch")
  }
}

val rustFlags: String = {
  rustTarget match {
    case "x86_64-unknown-linux-gnu"
         | "x86_64-apple-darwin"       => "-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit"
    case "aarch64-unknown-linux-gnu"   => "-Ctarget-cpu=ampere1"
    case "aarch64-apple-darwin"        => "-Ctarget-cpu=apple-m1"
    case other                         => sys.error(s"Unsupported Rust target: $other")
  }
}

val javaVersion: Int = {
  val parts = System.getProperty("java.version").split("\\.")

  // For Java 1.8, the version is "1.8.[..]", so we only use the 8
  if (parts(0) == "1")
    parts(1).toInt
  else
    parts(0).toInt
}

// -----------------------------------------------------------------------------
// Common Settings for All Projects
// -----------------------------------------------------------------------------
val baseJavaOpts = Seq(
  "-ea",
  "-Xmx4g",
  "-Xss4m",
  "-Djdk.reflect.useDirectMethodHandle=false"
)

val newerJavaOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

val commonJavaOpts = if (javaVersion > 9) baseJavaOpts ++ newerJavaOpts else baseJavaOpts

val commonSettings = Seq(
  javacOptions ++= {
    val version = javaVersion.toString
    Seq("-source", version, "-target", version)
  },
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-Xfatal-warnings",
    "-Ywarn-dead-code"
  ),
  Test / envVars := Map(
    "SPARK_HOME" -> baseDirectory.value.getParentFile.getAbsolutePath,
    "spark.test.home" -> baseDirectory.value.getParentFile.getAbsolutePath
  ),
  Test / fork := true, // Common doesn't use Comet itself, so fork is fine
  Test / parallelExecution := false,
  Test / publishArtifact := false,
  Test / testOptions += Tests.Argument("-oD"),
  Test / javaOptions ++= commonJavaOpts,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false }
)
// -----------------------------------------------------------------------------
// Common Assembly (Shading) Settings
// -----------------------------------------------------------------------------
val commonAssemblySettings = Seq(
  assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
  assembly / assemblyMergeStrategy := {
    import sbtassembly.{MergeStrategy, PathList}

    // Shading and merge hell, abandon all hope, ye who enter here
    {
      case PathList(ps@_*)
        if ps.last == "arrow-git.properties"                          => MergeStrategy.last
      case PathList("META-INF", "native", _*)                         => MergeStrategy.first
      case PathList("META-INF", "MANIFEST.MF")                        => MergeStrategy.discard
      case PathList("git.properties")                                 => MergeStrategy.discard
      case "module-info.class"                                        => MergeStrategy.discard
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case PathList(ps@_*) if ps.last == "module-info.class"          => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties")       => MergeStrategy.first
      case PathList("META-INF", "native-image", _*)                   => MergeStrategy.first
      case PathList("META-INF", "services", _*)                       => MergeStrategy.concat
      case "reference.conf"                                           => MergeStrategy.concat
      case PathList("META-INF", xs@_*)
        if xs.last.toLowerCase.endsWith(".properties")                => MergeStrategy.filterDistinctLines
      case PathList("META-INF", xs@_*)
        if xs.last.matches(".*\\.(sf|dsa|rsa)")                       => MergeStrategy.discard
      case x if x.endsWith(".proto") || x.endsWith(".thrift")         => MergeStrategy.discard
      case "log4j.properties" | "log4j2.properties"                   => MergeStrategy.discard
      case _                                                          => MergeStrategy.first
    }
  },

  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value

    cp.filter { entry =>
      val name = entry.data.getName
      name.endsWith(".so") || name.endsWith(".dylib") || name.endsWith(".dll") || name.contains("native-")
    }
  }
)

// -----------------------------------------------------------------------------
// Spark-Specific Source Helper
// -----------------------------------------------------------------------------
def sparkVersionSpecificSources(sparkVer: String, base: File): Seq[File] = {
  val Array(majorStr, minorStr, _*) = sparkVer.split("\\.")
  val major = majorStr.toInt
  val minor = minorStr.toInt

  val possibleDirs = Seq(
    base / s"spark-$sparkVer",
    base / s"spark-$major.$minor",
    base / s"spark-$major.x"
  )

  val maybePre35 =
    if (major == 3 && minor < 5) Seq(base / "spark-pre-3.5") else Nil

  (possibleDirs ++ maybePre35).filter(_.exists())
}

// -----------------------------------------------------------------------------
// Custom Tasks for Building Native Code, sbt-native isn't relevant since we still need Maven for publishing etc
// -----------------------------------------------------------------------------
lazy val cargoTask          = taskKey[Unit]("Builds Rust library (Cargo)")
lazy val copyNativeLibs     = taskKey[Seq[File]]("Copies .so/.dylib to resourceManaged")

// -----------------------------------------------------------------------------
// native Subproject (Rust-related tasks)
// -----------------------------------------------------------------------------
lazy val native = (project in file("native"))
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "native",

    cargoTask := {
      val log = streams.value.log
      val nativeDir = baseDirectory.value
      val buildType = sys.props.getOrElse("buildType", "debug")

      // To allow cross compilation
      log.info(s"Adding rust target: $rustTarget")
      if (Process(Seq("rustup", "target", "add", rustTarget)).! != 0)
        sys.error("rustup target add failed")

      val buildCmd = if (buildType == "release")
        Seq("cargo", "build", "--release", "--target", rustTarget)
      else
        Seq("cargo", "build", "--target", rustTarget)

      log.info(s"Building native library (target=$rustTarget, buildType=$buildType)")
      val exitCode = Process(buildCmd, nativeDir, "RUSTFLAGS" -> rustFlags).!

      if (exitCode != 0) sys.error(s"Cargo build failed with exit code $exitCode")
    },

    // Ensure cargoTask runs before copying the native libs to where they can be added to the assembly
    copyNativeLibs := {
      cargoTask.value

      val log = streams.value.log
      val buildType = sys.props.getOrElse("buildType", "debug")
      val resourceDir = (Compile / resourceManaged).value
      val nativeLibPath = s"org/apache/comet/$platform/$archFolder"
      val outDir = resourceDir / nativeLibPath
      IO.createDirectory(outDir)

      val libPattern = if (platform == "darwin") "libcomet.dylib" else "libcomet.so"
      val sourceDir = baseDirectory.value / "target" / rustTarget / (if (buildType == "release") "release" else "debug")

      val libs = (sourceDir * libPattern).get
      libs.map { lib =>
        val dest = outDir / lib.getName
        IO.copyFile(lib, dest)
        dest
      }
    },

    // Automatically package native libs as resources
    Compile / resourceGenerators += copyNativeLibs,
    Test    / resourceGenerators += copyNativeLibs,

    // Force Scala compile to wait for cargo tasks
    Compile / compile := (Compile / compile).dependsOn(copyNativeLibs).value,
  )

// -----------------------------------------------------------------------------
// common Subproject
// -----------------------------------------------------------------------------
lazy val common = (project in file("common"))
  .dependsOn(native)
  .disablePlugins(AssemblyPlugin) // No need for multiple assemblies
  .settings(commonSettings)
  .settings(
    name := "comet-common",

    Compile / unmanagedSourceDirectories ++=
      sparkVersionSpecificSources(sparkVersion, (Compile / sourceDirectory).value),

    libraryDependencies ++=
      sparkDependencies ++ Seq(
        "org.apache.parquet"  %   "parquet-column"  % parquetVersion,
        "org.apache.parquet"  %   "parquet-hadoop"  % parquetVersion,
        "org.apache.parquet"  %   "parquet-hadoop"  % parquetVersion classifier "tests",
        "junit"               %   "junit"           % "4.13.2"          % Test,
        "org.assertj"         %   "assertj-core"    % "3.24.2"          % Test,
        "org.scalatest"       %%  "scalatest"       % scalatestVersion  % Test
      ).map(_.excludeAll(
        ExclusionRule("org.apache.arrow"),
        ExclusionRule("com.google.guava", "guava"),
        ExclusionRule("commons-logging", "commons-logging")
      )) ++ arrowDependencies,

    // Minimal Git info resource
    Compile / resourceGenerators += Def.task {
      val outFile = (Compile / resourceManaged).value / "comet-git-info.properties"

      val branch      = Process("git rev-parse --abbrev-ref HEAD").!!.trim
      val commit      = Process("git rev-parse HEAD").!!.trim
      val shortCommit = Process("git rev-parse --short HEAD").!!.trim

      val content =
        s"""git.branch=$branch
           |git.commit.id.full=$commit
           |git.commit.id.abbrev=$shortCommit
           |""".stripMargin

      IO.write(outFile, content)
      Seq(outFile)
    }.taskValue
  )

// -----------------------------------------------------------------------------
// spark Subproject
// -----------------------------------------------------------------------------
lazy val spark = (project in file("spark"))
  .enablePlugins(ProtocPlugin)
  .dependsOn(common % "compile->compile;test->test", native)
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "spark",

    libraryDependencies ++= (
      sparkDependencies ++ arrowDependencies ++ testDependencies ++ Seq(
        "com.thesamet.scalapb"  %%  "scalapb-runtime"   % scalapbVersion,
        "com.google.protobuf"   %   "protobuf-java"     % protobufVersion,
        "com.google.guava"      %   "guava"             % "14.0.1",
        // Spark tests
        "org.apache.spark"      %% "spark-core"         % sparkVersion % "test" classifier "tests",
        "org.apache.spark"      %% "spark-catalyst"     % sparkVersion % "test" classifier "tests",
        "org.apache.spark"      %% "spark-sql"          % sparkVersion % "test" classifier "tests",
        "org.apache.spark"      %% "spark-hadoop-cloud" % sparkVersion % "test" classifier "tests"
      )
      ),

    javaOptions += s"-Djava.library.path=${baseDirectory.value}/../native/target/$rustTarget/debug",

    scalacOptions ++= Seq(
      "-Ypartial-unification",
      "-language:implicitConversions",
      "-language:existentials",
      "-nowarn"
    ),

    // ScalaPB / Protobuf
    Compile / PB.protocVersion := protobufVersion,
    Compile / PB.protoSources := Seq(
      (ThisBuild / baseDirectory).value / "native" / "proto" / "src" / "proto"
    ),
    Compile / PB.targets := Seq(
      PB.gens.java("java_multiple_files=false") ->
        (Compile / sourceManaged).value / "compiled_protobuf",
      scalapb.gen(
        Set(
          scalapb.GeneratorOption.JavaConversions,
          scalapb.GeneratorOption.SingleLineToProtoString,
          scalapb.GeneratorOption.Grpc
        )
      ) -> (Compile / sourceManaged).value / "scalapb"
    ),

    // Spark-version-specific sources
    Compile / unmanagedSourceDirectories ++=
      sparkVersionSpecificSources(sparkVersion, (Compile / sourceDirectory).value),
    Test    / unmanagedSourceDirectories ++=
      sparkVersionSpecificSources(sparkVersion, baseDirectory.value / "src" / "test"),

    // Ensure Protobuf generation happens first
    Compile / compile := (Compile / compile).dependsOn(Compile / PB.generate).value
  )

// -----------------------------------------------------------------------------
// spark-integration Subproject
// -----------------------------------------------------------------------------
lazy val sparkIntegration = (project in file("spark-integration"))
  .dependsOn(spark)
  .settings(commonSettings, commonAssemblySettings)
  .settings(
    name := "spark-integration",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
    ) ++ arrowDependencies,
  )

// -----------------------------------------------------------------------------
// fuzz-testing Subproject
// -----------------------------------------------------------------------------
lazy val fuzzTesting = (project in file("fuzz-testing"))
  .dependsOn(spark)
  .settings(commonSettings, commonAssemblySettings)
  .settings(
    name := "fuzz-testing",
    libraryDependencies ++= Seq(
      "org.scala-lang"    %   "scala-library" % scalaVersionUsed  % "provided",
      "org.apache.spark"  %%  "spark-sql"     % sparkVersion      % "provided",
      "org.rogach"        %%  "scallop"       % "4.1.0"
    ),
    Compile / mainClass := Some("org.apache.comet.FuzzTesting")
  )

// -----------------------------------------------------------------------------
// Root Project
// -----------------------------------------------------------------------------
lazy val root = (project in file("."))
  .aggregate(native, common, spark)
  .dependsOn(spark)
  .settings(commonSettings, commonAssemblySettings)
  .settings(
    name := "comet",

    // Shade rules to avoid conflicts
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> s"$cometShadePackage.protobuf.@1").inAll,
      ShadeRule.rename("com.google.common.**"   -> s"$cometShadePackage.guava.@1").inAll,
    ),

    // Skip tests during assembly
    assembly / test := {},
  )
