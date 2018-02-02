name := "curdaysearch"

version := "1.0"

scalaVersion := "2.10.5"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-annotations" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.codehaus.jackson" % "jackson-core-asl" % "1.9.8"
// https://mvnrepository.com/artifact/org.json/json
//libraryDependencies += "org.json" % "json" % "20160810"
libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.4"
packAutoSettings
