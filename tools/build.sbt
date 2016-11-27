name := "benchmarking"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"

retrieveManaged := true