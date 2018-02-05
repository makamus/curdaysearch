
package unicredit.spark.hbase

/**
  * Created by lxb on 1/31/2018.
  */
object FlightLineSpark {

}
//
//  val  dateformatglobal:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//
//  def md5Hash( srcStr:String): String = {
//    var digest  = java.security.MessageDigest.getInstance("MD5")
//    digest.digest(srcStr.getBytes).map("%02x".format(_)).mkString
//  }
//
//  def band3Str(str:String) :String = {
//    var ps = str
//    var k = 3-str.length
//    while(k>0){
//      ps="0"+ps
//      k=k-1
//    }
//    ps
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    var day:String = ""
//    var hour:Int = 0
//    if(args.length<4){
//      println( "Please Input correct style :  \"yyyy-MM-dd\" hour  filepath  zhangzhuan" )
//    }else if(args.length<2){
//      var cal = java.util.Calendar.getInstance()
//      cal.add(Calendar.HOUR_OF_DAY, -1)
//      day = dateformatglobal.format(cal.getTime)
//      hour = cal.get(Calendar.HOUR_OF_DAY)
//    }else{
//      day = args(0)
//      hour = args(1).toInt
//    }
//
//    var cal = java.util.Calendar.getInstance()
//    var afterYear:Int = 0
//    var curYear:Int= 0
//    try {
//      var year = Integer.parseInt(day.substring(0, 4))
//      cal.set(Calendar.YEAR, year)
//      curYear = cal.get(Calendar.YEAR)
//      cal.add(Calendar.YEAR, 1)
//      afterYear = cal.get(Calendar.YEAR)
//    }catch {
//      case e: Exception =>  println( "Please Input correct style :  \"yyyy-MM-dd\" hour  filepath " )
//    }
//
//
//    val sparkConf = new SparkConf().setAppName("FlightLine").setMaster("yarn-client")
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new HiveContext(sc)
//
//    val tableName = "flightline_day_price"
//    val configuration = HBaseConfiguration.create()
//    configuration.set("hbase.rootdir", "hdfs://sinobbd-data-01:8020/hbase")
//    configuration.setBoolean("hbase.cluster.distributed", true)
//    configuration.set("hbase.zookeeper.quorum", "172.29.100.22,172.29.100.23")
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
//    //val conn = ConnectionFactory.createConnection(conf)
//    // Initialize hBase table if necessary
//    val admin = new HBaseAdmin(configuration)
//    if (!admin.isTableAvailable(tableName+curYear)) {
//      val tableDesc = new HTableDescriptor(tableName+curYear)
//      tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
//      tableDesc.setValue("prefix_split_key_policy.prefix_length", "3");
//      tableDesc.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
//      tableDesc.setMaxFileSize(10 * 1024 * 1024 * 1024L)
//      val hcd = new HColumnDescriptor("basic")
//      hcd.setMaxVersions(24)
//      hcd.setMinVersions(0)
//      tableDesc.addFamily(hcd)
//      admin.createTable(tableDesc)
//    }
//    if (!admin.isTableAvailable(tableName+afterYear)) {
//      val tableDesc = new HTableDescriptor(tableName+afterYear)
//      tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
//      tableDesc.setValue("prefix_split_key_policy.prefix_length", "3");
//      tableDesc.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
//      tableDesc.setMaxFileSize(10 * 1024 * 1024 * 1024L)
//      val hcd = new HColumnDescriptor("basic")
//      hcd.setMaxVersions(24)
//      hcd.setMinVersions(0)
//      tableDesc.addFamily(hcd)
//      admin.createTable(tableDesc)
//    }
//
//    //创建表连接
//    val tableCurYear = new HTable(configuration, TableName.valueOf(tableName+curYear));
//    //将数据自动提交功能关闭
//    tableCurYear.setAutoFlush(true);
//    //设置数据缓存区域
//    tableCurYear.setWriteBufferSize(64 * 1024 * 1024);
//    //然后开始写入数据
//    val tableAfterYear = new HTable(configuration, TableName.valueOf(tableName+afterYear));
//    tableAfterYear.setAutoFlush(true)
//    tableAfterYear.setWriteBufferSize(64 * 1024 * 1024)
//
//    var rrrd = sqlContext.sql(s"select * from sp_class.iclog_zhongzhuan2 where dt='$day' and hour=$hour ").map(r => {
//      val departCity = r.getAs[String]("dc")
//      val arriveCity = r.getAs[String]("ac")
//      val departDay = r.getAs[String]("da")
//      var md5hash = md5Hash(departCity + arriveCity)
//      val dateFormat: SimpleDateFormat = new SimpleDateFormat("MMdd")
//      var departmonthday = dateFormat.format(dateformatglobal.parse(departDay) )
//
//      var calDepart = Calendar.getInstance()
//      calDepart.setTime(dateformatglobal.parse(departDay))
//      var calSearch = Calendar.getInstance()
//      calSearch.setTime(dateformatglobal.parse(day))
//      var diff = (calDepart.getTimeInMillis -  calSearch.getTimeInMillis )/ (1000*3600*24)
//      var diffday = String.valueOf(diff)
//
//      val key = md5hash.substring(0, 3) + departCity + arriveCity + departmonthday + band3Str(diffday)
//
//      val vp =
//
//      //(key, Seq(vp))
//      (key,Seq( (vp,hour.toLong) ) )
//    })
//
//    rrrd.collect().foreach( X => {
//      println(scala.util.parsing.json.JSONArray(X._2.head._1.toList) )
//    })
//
//    import sqlContext.implicits._
//    import org.apache.spark.sql.types._
//    import org.apache.spark.sql.functions._
//    import org.apache.spark.sql._
//    val df =sqlContext.read.json("/hive/warehouse/sp_class.db/iclog_zhongzhuan/*")
//    df.printSchema()
//    df.limit(1).select($"type", $"date", $"org", $"dst", explode($"datas") as "data")
//    df.foreach( r => { r} )
//    val mapper: ObjectMapper = new ObjectMapper
//    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
//    // This is really a Map<String, Object>. For more information about how
//    // Jackson parses JSON in this example, see
//    // http://wiki.fasterxml.com/JacksonDataBinding
//
//
//    val cols = Seq("zhongzhuan")
//    implicit var conf = HBaseConfig(configuration)
//
//    toHFileRDDFixedTS(rrrd).toHBaseBulk(tableName, "basic", cols)
//    admin.close()
//  }
//




//case class FlightRecord(t:Long,da:String, dc:String,ac:String , dl:
