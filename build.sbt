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
//		"org.apache.spark" %% "spark-sql"   % "2.0.1" % Provided,
	"org.apache.spark" %% "spark-mllib" % "2.0.1" % Provided,


	// test deps
	"org.apache.spark" %% "spark-mllib" % "2.0.1" % Test,
	"org.scalatest"    %% "scalatest"   % "2.2.5" % Test
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)