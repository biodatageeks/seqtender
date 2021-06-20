import scala.util.Properties

name := """seqtender"""

version := s"${sys.env.getOrElse("VERSION", "0.3.0")}"

organization := "org.biodatageeks"

scalaVersion := "2.12.8"

val DEFAULT_SPARK_3_VERSION = "3.1.1"
val DEFAULT_HADOOP_VERSION = "2.6.5"


lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_3_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)


libraryDependencies +=  "org.apache.spark" % "spark-core_2.12" % sparkVersion

libraryDependencies +=  "org.apache.spark" % "spark-sql_2.12" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion

libraryDependencies += "org.seqdoop" % "hadoop-bam" % "7.10.0"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll (ExclusionRule("org.apache.hadoop"))

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "test"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.8"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.github.samtools" % "htsjdk" % "2.21.1"

libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.17"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"

libraryDependencies += "com.intel.gkl" % "gkl" % "0.8.6"

libraryDependencies += "de.ruedigermoeller" % "fst" % "2.57"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.24.v20180605"
libraryDependencies += "org.apache.derby" % "derbyclient" % "10.14.2.0"

libraryDependencies += "org.disq-bio" % "disq" % "0.3.8"

libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.17" % Provided




fork := false
fork in Test := false
parallelExecution in Test := false


javaOptions in Test ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:+CMSClassUnloadingEnabled")

updateOptions := updateOptions.value.withLatestSnapshots(false)

outputStrategy := Some(StdoutOutput)


resolvers ++= Seq(
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "zsibio-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/",
  "spring" at "https://repo.spring.io/libs-milestone/",
  "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "komiya" at "https://dl.bintray.com/komiya-atsushi/maven"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishTo := {
  if (!version.value.toLowerCase.contains("snapshot"))
    sonatypePublishToBundle.value
  else {
    val nexus = "https://zsibio.ii.pw.edu.pl/nexus/repository/"
    Some("snapshots" at nexus + "maven-snapshots")
  }
}