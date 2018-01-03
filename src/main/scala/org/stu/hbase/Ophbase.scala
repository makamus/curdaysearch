package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lxb on 2017/12/28.
  */
object Ophbase {
  val v =  "{\"cal\": \"0\",\"sh\": \"1\",\"airc\": \"MU\",\"shairc\": \"联航\",\"sno\": \"KN5899\",\"st\": \"17:05\",\"et\": \"20:25\",\"no\": \"MU3825\",\"tp\": \"73V(中)\",\"sc\": \"NAY\",\"ec\": \"CAN\",\"eter\": \"B区\",\"le\": \"0\",\"cs\": [{\"c\": \"5\",\"fp\": \"1570\",\"cw\": \"E\",\"dis\": \"8.2\",\"tn\": \"100\"},{\"c\": \"4\",\"fp\": \"1970\",\"cw\": \"E\",\"dis\": \"9.2\",\"tn\": \"100\"}]}"

  def md5Hash( srcStr:String): String = {
    var digest  = java.security.MessageDigest.getInstance("MD5")
    digest.digest(srcStr.getBytes).map("%02x".format(_)).mkString
  }

  def rand3Str() :String = {
    var ps=(new java.util.Random).nextInt(365).toString
    var k = 3-ps.length
    while(k>0){
      ps="0"+ps
      k=k-1
    }
    ps
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Ophbase").setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    /*val tablename = "actest"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","10.0.1.190,10.0.1.188,10.0.1.189")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("K1,jack,15","K2,Lily,16","K3,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("base"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("base"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())*/
    val tableName = "airline_search_broad"
    val tableName_high = "airline_search_high"
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.rootdir", "hdfs://sinobbd-data-01:8020/hbase")
    configuration.setBoolean("hbase.cluster.distributed", true)
    configuration.set("hbase.zookeeper.quorum", "172.29.100.22,172.29.100.23")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    //val conn = ConnectionFactory.createConnection(conf)
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(configuration)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
      tableDesc.setValue("prefix_split_key_policy.prefix_length", "3");
      tableDesc.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
      tableDesc.setMaxFileSize(10*1024 * 1024 * 1024L)
      val hcd = new HColumnDescriptor("basic")
      hcd.setMaxVersions(24)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }
    if (!admin.isTableAvailable(tableName_high)) {
      val tableDesc = new HTableDescriptor(tableName_high)
      tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
      tableDesc.setValue("prefix_split_key_policy.prefix_length", "3");
      tableDesc.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
      tableDesc.setMaxFileSize(10*1024 * 1024 * 1024L)
      val hcd = new HColumnDescriptor("basic")
      hcd.setMaxVersions(24)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }
    var i=0;
    var rrd : RDD[(String, Seq[(String,Long)])] = null;
    val rand:String = rand3Str
    while(i<200) {
      var rrrd = sqlContext.table("sp_class.flight_route").map(r => {
        val scode = r.getAs[String]("scode")
        val ecode = r.getAs[String]("ecode")
        var md5hash = md5Hash(scode + ecode)
        var  dateFormat:SimpleDateFormat = new SimpleDateFormat("MMdd")
        var cal:Calendar=Calendar.getInstance()
        cal.add(Calendar.DATE,- (new java.util.Random).nextInt(200))
        var yesterday=dateFormat.format(cal.getTime())
        val key = md5hash.substring(0, 3) + scode + ecode + yesterday + rand3Str
        var random = (new java.util.Random).nextInt(15)
        if(random<3) random=3;
        var j=1;
        var vp=v
        while(j<=random){
          vp=vp+","+v
          j=j+1
        }
        //val ts = (new java.util.Random).nextInt(23).toLong
        vp = "{\"fl\": [" + vp + "]}"
        j=1;
        var seq_arr : Array[(String,Seq[(String,Long)])] = Array[(String,Seq[(String,Long)])]()
        while(j<=random){
          val ts = (new java.util.Random).nextInt(23).toLong
          seq_arr = seq_arr :+ (key,Seq( (vp,ts) ) )
          j=j+1
        }

        seq_arr
      })

      val rdv = rrrd.flatMap(y=>y)

      i=i+1
      if(rrd==null){
        rrd=rdv
      }else{
        rrd = rrd++rdv
      }
    }

    val rrd_broad = rrd.map( X => {
      (X._1.substring(0, 13), X._2)
    })


    val cols = Seq("linedata")
    val cols_broad = Seq( rand )
    implicit var conf = HBaseConfig(configuration)
    toHFileRDDFixedTS(rrd_broad).toHBaseBulk(tableName, "basic", cols_broad)
    toHFileRDDFixedTS(rrd).toHBaseBulk(tableName_high, "basic", cols)


    admin.close()
  }
}
