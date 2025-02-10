import scala.sys.process._

import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.autoImport.PB

// -----------------------------------------------------------------------------
// Root / Global Settings
// -----------------------------------------------------------------------------
// Currently this needs to be modified manually, which isn't great but works for now
ThisBuild / version := "0.6.0-SNAPSHOT"
ThisBuild / organization := "org.apache.datafusion"

// Global Repositories + Dep Overrides
ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.sonatypeOssRepos("releases").head,
  Resolver.sonatypeOssRepos("snapshots").head,
  "Apache Arrow" at "https://arrow.apache.org/docs/java/").map(_.withAllowInsecureProtocol(true))

// Override Jackson versions
ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2")

// -----------------------------------------------------------------------------
// Version Variables, I tried to follow the pom.xml as much as I could
// -----------------------------------------------------------------------------
lazy val sparkVersion = settingKey[String]("Apache Spark version")
lazy val parquetVersion = settingKey[String]("Apache Parquet version")
lazy val slf4jVersion = settingKey[String]("SLF4J version")
lazy val javaTarget = settingKey[Int]("Java target version")
lazy val sparkScalaVersion = settingKey[String]("Default Scala version for the selected Spark version")

ThisBuild / parquetVersion := "1.13.1"
ThisBuild / javaTarget := 0
ThisBuild / sparkScalaVersion := ""
ThisBuild / sparkVersion := {
  val sparkProps = sys.props.map(_._1).filter(_.startsWith("spark")).toSeq
  sparkProps.size match {
    case 0 => {
      ThisBuild / sparkScalaVersion := "2.12.17"
      "3.4.3"
    }
    case 1 => {
      val prop = sparkProps(0)
      prop match {
        case "spark3.3" => {
          ThisBuild / sparkScalaVersion := "2.12.15"
          ThisBuild / parquetVersion := "1.12.0"
          ThisBuild / slf4jVersion := "1.7.32"
          "3.3.2"
        }
        case "spark3.4" => {
          ThisBuild / sparkScalaVersion := "2.12.17"
          "3.4.3"
        }
        case "spark3.5" => {
          ThisBuild / sparkScalaVersion := "2.12.18"
          ThisBuild / slf4jVersion := "2.0.7"
          "3.5.1"
        }
        case "spark4.0" => {
          ThisBuild / sparkScalaVersion := "2.12.14"
          ThisBuild / semanticdbVersion := "4.9.5"
          ThisBuild / slf4jVersion := "2.0.13"
          ThisBuild / javaTarget := 17
          "4.0.0-preview1"
        }
        case other => sys.error(s"Unsupported Spark version: $other")
      }
    }
    case other => sys.error(s"Multiple spark definitions provided: $sparkProps")
  }
}

ThisBuild / scalaVersion := {
  val scalaProps = sys.props.map(_._1).filter(_.startsWith("scala")).toSeq
  scalaProps.size match {
    case 0 => {
      // Use value specified by selected Spark version
      // If none such exists, use SBT's default
      if ((ThisBuild / sparkScalaVersion).value == "") {
        "2.12.17"
      } else {
        (ThisBuild / sparkScalaVersion).value
      }
    }
    case 1 => {
      scalaProps(0) match {
        case "scala2.12" | "" =>
          // If the spark version selected a 2.12 Scala, use it, otherwise use 2.12.17
          if ((ThisBuild / sparkScalaVersion).value.startsWith("2.12")) {
            (ThisBuild / sparkScalaVersion).value
          } else {
            "2.12.17"
          }
        case "scala2.13" =>
          ThisBuild / semanticdbVersion := "4.9.5"
          "2.13.14"
        case other => sys.error(s"Unsupported Scala version: $other")
      }
    }
    case other => sys.error(s"More than one Scala version definitions provided: $scalaProps")
  }
}

val arrowVersion = "16.0.0"
val scalatestVersion = "3.2.9"
val protobufVersion = "3.21.12"
val scalapbVersion = "0.11.13"
val cometShadePackage = "org.apache.comet.shaded"

// -----------------------------------------------------------------------------
// Common Dependencies
// -----------------------------------------------------------------------------
val sparkDependencies = Def.setting {
  Seq(
    "org.apache.spark" %% "spark-core" % (ThisBuild / sparkVersion).value,
    "org.apache.spark" %% "spark-sql" % (ThisBuild / sparkVersion).value,
    "org.apache.spark" %% "spark-catalyst" % (ThisBuild / sparkVersion).value,
    "org.apache.spark" %% "spark-network-common" % (ThisBuild / sparkVersion).value)
    .map(
      _ % "provided" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ))
}

val arrowDependencies = Seq(
  "org.apache.arrow" % "arrow-vector" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-core" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-unsafe" % arrowVersion,
  "org.apache.arrow" % "arrow-format" % arrowVersion,
  "org.apache.arrow" % "arrow-c-data" % arrowVersion)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalatestplus" %% "junit-4-13" % "3.2.9.0",
  "junit" % "junit" % "4.13.2",
  "org.assertj" % "assertj-core" % "3.24.2").map(_ % Test)

// -----------------------------------------------------------------------------
// OS / Architecture / Java Detection
// -----------------------------------------------------------------------------
lazy val platform = settingKey[String]("OS Family to build for")
lazy val archFolder = settingKey[String]("Architecture to build for")
lazy val rustTarget = settingKey[String]("Rust target and toolchain to use")
ThisBuild / rustTarget := {
  val platformProps = sys.props.map(_._1).filter {
    x => x == "Win-x86" || x == "Darwin-x86" || x == "Darwin-aarch64" || x == "Linux-amd64" || x == "Linux-aarch64"
  }.toSeq

  platformProps.size match {
    case 0 => {
      val family = System.getProperty("os.name").toLowerCase
      val arch = System.getProperty("os.arch").toLowerCase

      val localPlatform =
        if (family.contains("linux")) "linux"
        else if (family.contains("mac") || family.contains("darwin")) "darwin"
        else if (family.contains("win")) "windows" // Handled "darwin" cases earlier
        else sys.error(s"Unsupported OS: $family")

      ThisBuild / platform := localPlatform

      val localArchFolder =
        if (arch == "amd64" || arch == "x86_64") "amd64"
        else if (arch == "aarch64" || arch == "arm64") "aarch64"
        else sys.error(s"Unsupported architecture: $arch")

      ThisBuild / archFolder := localArchFolder

      (localPlatform, localArchFolder) match {
        case ("linux", "amd64") => "x86_64-unknown-linux-gnu"
        case ("linux", "aarch64") => "aarch64-unknown-linux-gnu"
        case ("darwin", "amd64") => "x86_64-apple-darwin"
        case ("darwin", "aarch64") => "aarch64-apple-darwin"
        case ("windows", "amd64") => "x86_64-pc-windows-msvc"
        case (otherPlatform, otherArch) =>
          sys.error(s"Unsupported platform/arch: $otherPlatform/$otherArch")
      }
    }
    case 1 => {
      platformProps(0) match {
        case "Win-x86" => {
          ThisBuild / platform := "windows"
          ThisBuild / archFolder := "amd64"
          "x86_64-pc-windows-msvc"
        }
        case "Darwin-x86" => {
          ThisBuild / platform := "darwin"
          ThisBuild / archFolder := "amd64"
          "x86_64-apple-darwin"
        }
        case "Darwin-aarch64" => {
          ThisBuild / platform := "darwin"
          ThisBuild / archFolder := "aarch64"
          "aarch64-apple-darwin"
        }
        case "Linux-amd64" => {
          ThisBuild / platform := "linux"
          ThisBuild / archFolder := "amd64"
          "x86_64-unknown-linux-gnu"
        }
        case "Linux-aarch64" => {
          ThisBuild / platform := "linux"
          ThisBuild / archFolder := "aarch64"
          "aarch64-unknown-linux-gnu"
        }
      }
    }
    case other => sys.error(s"Multiple platform definitions provided: $platformProps")
  }
}

lazy val rustFlags = settingKey[String]("Which flags to pass to the Rust compiler")
ThisBuild / rustFlags := {
  (ThisBuild / rustTarget).value match {
    case "x86_64-pc-windows-msvc" | "x86_64-unknown-linux-gnu" | "x86_64-apple-darwin" =>
      "-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit"
    case "aarch64-unknown-linux-gnu" => "-Ctarget-cpu=ampere1"
    case "aarch64-apple-darwin" => "-Ctarget-cpu=apple-m1"
    case other => sys.error(s"Unsupported Rust target: $other")
  }
}

ThisBuild / javaTarget := {
  val jdkProps = sys.props.map(_._1).filter(_.startsWith("jdk")).toSeq
  jdkProps.size match {
    case 0 => {
      // If Spark version set a default, use it, otherwise, take the System property
      if ((ThisBuild / javaTarget).value == 0) {
        val parts = System.getProperty("java.version").split("\\.")

        // Handle JDK8
        if (parts(0) == "1")
          parts(1).toInt
        else
          parts(0).toInt
      } else (ThisBuild / javaTarget).value
    }
    case 1 => {
      jdkProps(0) match {
        case "jdk1.8" => 8
        case "jdk11" => 11
        case "jdk17" => 17
        case other => sys.error(s"Unsupported JDK profile selected: $other")
      }
    }
    case other => sys.error(s"Multiple JDK definitions provided: $jdkProps")
  }
}

// -----------------------------------------------------------------------------
// Common Settings for All Projects
// -----------------------------------------------------------------------------
val baseJavaOpts = Seq("-ea", "-Xmx4g", "-Xss4m", "-Djdk.reflect.useDirectMethodHandle=false")

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
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")

val commonSettings = Seq(
  javacOptions ++= {
    val version = (ThisBuild / javaTarget).value.toString
    Seq("-target", version)
  },
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-Xfatal-warnings",
    "-Ywarn-dead-code"),
  Test / envVars := Map(
    "SPARK_HOME" -> baseDirectory.value.getParentFile.getAbsolutePath,
    "spark.test.home" -> baseDirectory.value.getParentFile.getAbsolutePath),
  Test / fork := true, // Common doesn't use Comet itself, so fork is fine
  Test / parallelExecution := false,
  Test / publishArtifact := false,
  Test / testOptions += Tests.Argument("-oD"),
  Test / javaOptions ++= {
    if ((ThisBuild / javaTarget).value > 9) {
      baseJavaOpts ++ newerJavaOpts
    } else {
      baseJavaOpts
    }
  },
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false })
// -----------------------------------------------------------------------------
// Common Assembly (Shading) Settings
// -----------------------------------------------------------------------------
val commonAssemblySettings = Seq(
  assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
  assembly / assemblyMergeStrategy := {
    import sbtassembly.{MergeStrategy, PathList}

    // Shading and merge hell, abandon all hope, ye who enter here
    {
      case PathList(ps @ _*) if ps.last == "arrow-git.properties" => MergeStrategy.last
      case PathList("META-INF", "native", _*) => MergeStrategy.first
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("git.properties") => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", "native-image", _*) => MergeStrategy.first
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) if xs.last.toLowerCase.endsWith(".properties") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", xs @ _*) if xs.last.matches(".*\\.(sf|dsa|rsa)") =>
        MergeStrategy.discard
      case x if x.endsWith(".proto") || x.endsWith(".thrift") => MergeStrategy.discard
      case "log4j.properties" | "log4j2.properties" => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  },
  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value

    cp.filter { entry =>
      val name = entry.data.getName
      name.endsWith(".so") || name.endsWith(".dylib") || name.endsWith(".dll") || name.contains(
        "native-")
    }
  })

// -----------------------------------------------------------------------------
// Spark-Specific Source Helper
// -----------------------------------------------------------------------------
// This helps with the various shims
def sparkVersionSpecificSources(sparkVer: String, base: File): Seq[File] = {
  val Array(majorStr, minorStr, _*) = sparkVer.split("\\.")
  val major = majorStr.toInt
  val minor = minorStr.toInt

  val possibleDirs =
    Seq(base / s"spark-$sparkVer", base / s"spark-$major.$minor", base / s"spark-$major.x")

  val maybePre35 =
    if (major == 3 && minor < 5) Seq(base / "spark-pre-3.5") else Nil

  (possibleDirs ++ maybePre35).filter(_.exists())
}

// -----------------------------------------------------------------------------
// Custom Tasks for Building Native Code, sbt-native isn't relevant since we still need Maven for publishing etc
// -----------------------------------------------------------------------------
lazy val cargoTask = taskKey[Unit]("Builds Rust library (Cargo)")
lazy val copyNativeLibs = taskKey[Seq[File]]("Copies .so/.dylib/.dll to resourceManaged")

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
      val buildType = if (sys.props.contains("release") || sys.props.contains("apache-release")) "release" else "debug"

      // To allow cross compilation
      val selectedTarget = (ThisBuild / rustTarget).value
      log.info(s"Adding rust target: $selectedTarget")
      if (Process(Seq("rustup", "target", "add", selectedTarget)).! != 0)
        sys.error("rustup target add failed")

      val buildCmd =
        if (buildType == "release")
          Seq("cargo", "build", "--release", "--target", selectedTarget)
        else
          Seq("cargo", "build", "--target", selectedTarget)

      log.info(s"Building native library (target=$selectedTarget, buildType=$buildType)")
      val exitCode = Process(buildCmd, nativeDir, "RUSTFLAGS" -> (ThisBuild / rustFlags).value).!

      if (exitCode != 0) sys.error(s"Cargo build failed with exit code $exitCode")
    },

    // Ensure cargoTask runs before copying the native libs to where they can be added to the assembly
    copyNativeLibs := {
      cargoTask.value

      val log = streams.value.log
      val buildType = if (sys.props.contains("release") || sys.props.contains("apache-release")) "release" else "debug"
      val resourceDir = (Compile / resourceManaged).value
      val nativeLibPath = s"org/apache/comet/$platform/$archFolder"
      val outDir = resourceDir / nativeLibPath
      IO.createDirectory(outDir)

      val libPattern =
        if (platform == "darwin") "libcomet.dylib"
        else if (platform == "windows") "libcomet.dll"
        else "libcomet.so"
      val sourceDir =
        baseDirectory.value / "target" / (ThisBuild / rustTarget).value / (if (buildType == "release")
                                                                             "release"
                                                                           else "debug")

      val libs = (sourceDir * libPattern).get
      libs.map { lib =>
        val dest = outDir / lib.getName
        IO.copyFile(lib, dest)
        dest
      }
    },

    // Automatically package native libs as resources
    Compile / resourceGenerators += copyNativeLibs,
    Test / resourceGenerators += copyNativeLibs,

    // Force Scala compile to wait for cargo tasks
    Compile / compile := (Compile / compile).dependsOn(copyNativeLibs).value)

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
      sparkVersionSpecificSources(
        (ThisBuild / sparkVersion).value,
        (Compile / sourceDirectory).value),
    libraryDependencies ++=
      sparkDependencies.value ++ Seq(
        "org.apache.parquet" % "parquet-column" % (ThisBuild / parquetVersion).value,
        "org.apache.parquet" % "parquet-hadoop" % (ThisBuild / parquetVersion).value,
        "org.apache.parquet" % "parquet-hadoop" % (ThisBuild / parquetVersion).value classifier "tests",
        "junit" % "junit" % "4.13.2" % Test,
        "org.assertj" % "assertj-core" % "3.24.2" % Test,
        "org.scalatest" %% "scalatest" % scalatestVersion % Test).map(
        _.excludeAll(
          ExclusionRule("org.apache.arrow"),
          ExclusionRule("com.google.guava", "guava"),
          ExclusionRule("commons-logging", "commons-logging"))) ++ arrowDependencies,

    // Minimal Git info resource
    Compile / resourceGenerators += Def.task {
      val outFile = (Compile / resourceManaged).value / "comet-git-info.properties"

      val branch = Process("git rev-parse --abbrev-ref HEAD").!!.trim
      val commit = Process("git rev-parse HEAD").!!.trim
      val shortCommit = Process("git rev-parse --short HEAD").!!.trim

      val content =
        s"""git.branch=$branch
           |git.commit.id.full=$commit
           |git.commit.id.abbrev=$shortCommit
           |""".stripMargin

      IO.write(outFile, content)
      Seq(outFile)
    }.taskValue)

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
      sparkDependencies.value ++ arrowDependencies ++ testDependencies ++ Seq(
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
        "com.google.protobuf" % "protobuf-java" % protobufVersion,
        "com.google.guava" % "guava" % "14.0.1",
        // Spark tests
        "org.apache.spark" %% "spark-core" % (ThisBuild / sparkVersion).value % "test" classifier "tests",
        "org.apache.spark" %% "spark-catalyst" % (ThisBuild / sparkVersion).value % "test" classifier "tests",
        "org.apache.spark" %% "spark-sql" % (ThisBuild / sparkVersion).value % "test" classifier "tests",
        "org.apache.spark" %% "spark-hadoop-cloud" % (ThisBuild / sparkVersion).value % "test" classifier "tests")
    ),
    javaOptions += s"-Djava.library.path=${baseDirectory.value}/../native/target/$rustTarget/debug",
    scalacOptions ++= Seq(
      "-Ypartial-unification",
      "-language:implicitConversions",
      "-language:existentials",
      "-nowarn"),

    // ScalaPB / Protobuf
    Compile / PB.protocVersion := protobufVersion,
    Compile / PB.protoSources := Seq(
      (ThisBuild / baseDirectory).value / "native" / "proto" / "src" / "proto"),
    Compile / PB.targets := Seq(
      PB.gens.java("java_multiple_files=false") ->
        (Compile / sourceManaged).value / "compiled_protobuf",
      scalapb.gen(
        Set(
          scalapb.GeneratorOption.JavaConversions,
          scalapb.GeneratorOption.SingleLineToProtoString,
          scalapb.GeneratorOption.Grpc)) -> (Compile / sourceManaged).value / "scalapb"),

    // Spark-version-specific sources
    Compile / unmanagedSourceDirectories ++=
      sparkVersionSpecificSources(
        (ThisBuild / sparkVersion).value,
        (Compile / sourceDirectory).value),
    Test / unmanagedSourceDirectories ++=
      sparkVersionSpecificSources(
        (ThisBuild / sparkVersion).value,
        baseDirectory.value / "src" / "test"),

    // Ensure Protobuf generation happens first
    Compile / compile := (Compile / compile).dependsOn(Compile / PB.generate).value)

// -----------------------------------------------------------------------------
// spark-integration subproject, honestly not sure what this even does
// -----------------------------------------------------------------------------
lazy val sparkIntegration = (project in file("spark-integration"))
  .dependsOn(spark)
  .settings(commonSettings, commonAssemblySettings)
  .settings(
    name := "spark-integration",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % (ThisBuild / sparkVersion).value % "provided") ++ arrowDependencies)

// -----------------------------------------------------------------------------
// fuzz-testing Subproject, left it to assemble separately
// -----------------------------------------------------------------------------
lazy val fuzzTesting = (project in file("fuzz-testing"))
  .dependsOn(spark)
  .settings(commonSettings, commonAssemblySettings)
  .settings(
    name := "fuzz-testing",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % (ThisBuild / scalaVersion).value % "provided",
      "org.apache.spark" %% "spark-sql" % (ThisBuild / sparkVersion).value % "provided",
      "org.rogach" %% "scallop" % "4.1.0"),
    Compile / mainClass := Some("org.apache.comet.FuzzTesting"))

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
      ShadeRule.rename("com.google.common.**" -> s"$cometShadePackage.guava.@1").inAll),

    // Skip tests during assembly
    assembly / test := {})
