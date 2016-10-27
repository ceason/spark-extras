name := "mllib-extras"

version := "1.0"

scalaVersion := "2.11.8"



dependencyOverrides ++= Set( /** @see [[https://github.com/sbt/sbt/issues/2286]] */
	"org.scala-lang" % "scala-compiler" % scalaVersion.value,
	"org.scala-lang" % "scala-library"  % scalaVersion.value,
	"org.scala-lang" % "scala-reflect"  % scalaVersion.value
)

libraryDependencies ++= Seq(

	// general
	"com.google.inject" %  "guice"            % "3.0",
	"org.slf4j"         %  "slf4j-api"        % "1.7.10",
	"org.luaj"          %  "luaj-jse"         % "3.0.1",
//	"com.univocity"     % "univocity-parsers" % "2.2.2",
//	"com.github.nscala-time" %% "nscala-time" % "2.14.0",

	// spark
//		"org.apache.spark" %% "spark-core"  % "2.0.1" % Provided,
//	"org.apache.spark" %% "spark-hive-thriftserver" % "2.0.1" % Provided,
	"org.apache.spark" %% "spark-mllib"             % "2.0.1", // % Provided,


	// proto
	"com.trueaccord.scalapb" %% "scalapb-runtime"      % "0.5.43" % "protobuf",

	// viz
	"org.vegas-viz" %% "vegas"       % "0.3.6",
	"org.vegas-viz" %% "vegas-spark" % "0.3.6",


	// test deps
	"org.apache.spark" %% "spark-mllib" % "2.0.1" % Test,
	"org.scalatest"    %% "scalatest"   % "2.2.5" % Test
)

PB.targets in Compile := Seq(
//	PB.gens.java → (sourceManaged in Compile).value,
	scalapb.gen(javaConversions = false, grpc = false) → (sourceManaged in Compile).value
)

PB.pythonExe := "C:\\Python27\\Python.exe"


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)