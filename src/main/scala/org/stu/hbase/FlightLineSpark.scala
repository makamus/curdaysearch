
package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by lxb on 1/31/2018.
  */
object FlightLineSpark {

  val garr:Array[_] = Array(
    Map("name" -> "dorothy", "age" -> 5, "hasChild" -> false),
    Map("name" -> "bill", "age" -> 8, "hasChild" -> false) );

  val dateformatglobal: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def md5Hash(srcStr: String): String = {
    var digest = java.security.MessageDigest.getInstance("MD5")
    digest.digest(srcStr.getBytes).map("%02x".format(_)).mkString
  }

  def band3Str(str: String): String = {
    var ps = str
    var k = 3 - str.length
    while (k > 0) {
      ps = "0" + ps
      k = k - 1
    }
    ps
  }

  def main(args: Array[String]): Unit = {

    var day: String = ""
    var hour: Int = 0
    if (args.length < 4) {
      println("Please Input correct style :  \"yyyy-MM-dd\" hour  filepath  zhongzhuan")
      System.exit(0)
    } else if (args.length < 2) {
      var cal = java.util.Calendar.getInstance()
      cal.add(Calendar.HOUR_OF_DAY, -1)
      day = dateformatglobal.format(cal.getTime)
      hour = cal.get(Calendar.HOUR_OF_DAY)
    } else {
      day = args(0)
      hour = args(1).toInt
    }

    var cal = java.util.Calendar.getInstance()
    var afterYear: Int = 0
    var curYear: Int = 0
    try {
      var year = Integer.parseInt(day.substring(0, 4))
      cal.set(Calendar.YEAR, year)
      curYear = cal.get(Calendar.YEAR)
      cal.add(Calendar.YEAR, 1)
      afterYear = cal.get(Calendar.YEAR)
    } catch {
      case e: Exception => println("Please Input correct style :  \"yyyy-MM-dd\" hour  filepath ")
    }


    val sparkConf = new SparkConf().setAppName("FlightLineSpark").setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val tableName = "flightline_day_price"
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
      tableDesc.setMaxFileSize(10 * 1024 * 1024 * 1024L)
      val hcd = new HColumnDescriptor("basic")
      hcd.setMaxVersions(24)
      hcd.setMinVersions(0)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }


    //创建表连接
    val tableCurYear = new HTable(configuration, TableName.valueOf(tableName));
    //将数据自动提交功能关闭
    tableCurYear.setAutoFlush(true);
    //设置数据缓存区域
    tableCurYear.setWriteBufferSize(64 * 1024 * 1024);
    //然后开始写入数据


    var rrd = sqlContext.sql(s"select * from sp_class.iclog_zhongzhuan2 where dt='$day' and hour=$hour ").map(r => {
      val departCity = r.getAs[String]("dc")
      val arriveCity = r.getAs[String]("ac")
      val departDay = r.getAs[String]("da")
      var md5hash = md5Hash(departCity + arriveCity)
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("MMdd")
      var departmonthday = dateFormat.format(dateformatglobal.parse(departDay))

      var calDepart = Calendar.getInstance()
      calDepart.setTime(dateformatglobal.parse(departDay))
      var calSearch = Calendar.getInstance()
      calSearch.setTime(dateformatglobal.parse(day))
      var diff = (calDepart.getTimeInMillis - calSearch.getTimeInMillis) / (1000 * 3600 * 24)
      var diffday = String.valueOf(diff)

      val key = md5hash.substring(0, 3) + departCity + arriveCity + departmonthday + band3Str(diffday)

      val mapper: ObjectMapper = new ObjectMapper
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      mapper.registerModule(DefaultScalaModule)

      //val vp = mapper.writeValueAsString(  garr )
      val vv = mapper.writeValueAsString(r.getAs[Seq[org.apache.spark.sql.Row]]("dl").map( r=> {
        val fi= r.getAs[Seq[Row]]("fi").map( x=> {
          val mpfiele= Map(
            "de" -> x.getAs[String]("de"),
            "ar"-> x.getAs[String]("ar"),
            "dt"->x.getAs[String]("dt"),
            "fn"->x.getAs[String]("fn"),
            "ac"->x.getAs[String]("ac"),
            "at"->x.getAs[String]("at"),
            "et"->x.getAs[String]("et") );
          (mpfiele)
        });
        val pi= r.getAs[Seq[Row]]("pi").map( x=> {
          val mppiele = Map(
          "ft"->x.getAs[String]("ft"),
          "st"->x.getAs[String]("st"),
          "sr"->x.getAs[String]("sr") );
          (mppiele)
        });
        Map("fi"->fi, "pi"->pi)
      }) )
      (vv)
      //(key, Seq((vp, hour.toLong)))
    })

    rrd.saveAsTextFile("/user/bigdata/dd")
    /*val df = sqlContext.read.json("/user/bigdata/iclog_zhongzhuan_2.2018-02-02-16")
    df.printSchema()
    //df.limit(1).select($"type", $"date", $"org", $"dst", explode($"datas") as "data")

    // This is really a Map<String, Object>. For more information about how
    // Jackson parses JSON in this example, see
    // http://wiki.fasterxml.com/JacksonDataBinding
    val rrd = df.map(r => {
      val departCity = r.getAs[String]("dc")
      val arriveCity = r.getAs[String]("ac")
      val departDay = r.getAs[String]("da")
      val mapper: ObjectMapper = new ObjectMapper
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
      mapper.registerModule(DefaultScalaModule)
      val kk = r.get(3).getClass.getName + mapper.writeValueAsString( r.get(3).asInstanceOf[mutable.WrappedArray[_]].toArray )

      val vp:String = mapper.writeValueAsString(r.getAs("dl").asInstanceOf[Seq[_]].toList)

      var md5hash = md5Hash(departCity + arriveCity)

      val dateFormat: SimpleDateFormat = new SimpleDateFormat("MMdd")
      var departmonthday = dateFormat.format(dateformatglobal.parse(departDay))

      var calDepart = Calendar.getInstance()
      calDepart.setTime(dateformatglobal.parse(departDay))
      var calSearch = Calendar.getInstance()
      calSearch.setTime(dateformatglobal.parse(day))
      var diff = (calDepart.getTimeInMillis - calSearch.getTimeInMillis) / (1000 * 3600 * 24)
      var diffday = String.valueOf(diff)

      val key:String = md5hash.substring(0, 3) + departCity + arriveCity + departmonthday + band3Str(diffday)
      //( key, Seq((vp, hour.toLong)) )
      (kk)
    })
    rrd.saveAsTextFile("/user/bigdata/dd")*/

    val cols = Seq("zhongzhuan")
    implicit var conf = HBaseConfig(configuration)
    //toHFileRDDFixedTS(rrd).toHBaseBulk(tableName, "basic", cols)
    admin.close()
  }

}



//case class FlightRecord(t:Long,da:String, dc:String,ac:String , dl:
