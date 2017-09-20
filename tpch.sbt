name := "Spark TPC-H Queries"

version := "1.1"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"