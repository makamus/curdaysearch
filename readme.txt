hbase dependency:
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.10.0"
libraryDependencies += "org.apache.hbase" % "hbase-annotations" % "1.2.0-cdh5.10.0"

JAVA maven project should add  repository:
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>

org.apache.hadoop.hbase.client.HConnection conn= CurSearch.getHBaseConn()         //this connection pool , init with serving-startup

 CurSearch.getUserCurDay(conn,"xxD/ojlzt4npRKugX208cQ==")		// it has a light-weight HTableInterface open and close in method 