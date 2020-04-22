name := "workflow-example"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"


libraryDependencies := Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  // "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.github.pureconfig" %% "pureconfig" % "0.12.0",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
