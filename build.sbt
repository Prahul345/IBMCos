name := "com.spark.ibm"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.4" ,"org.apache.hadoop" % "hadoop-client" % "2.7.3","org.apache.spark" %% "spark-sql" % "2.4.3","com.ibm.db2" % "jcc" % "11.1.4.4",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3","com.ibm.stocator" % "stocator" % "1.0.24")