name := "DataAnaysis"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4" % "test"
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.12.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.11"
// libraryDependencies += "org.apache.jena" % "apache-jena-libs" % "3.7.0"