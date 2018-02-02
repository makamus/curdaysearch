package org.stu.hbase

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lxb on 12/29/2017.
  */
object Ophbase2 {
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
    val sparkConf = new SparkConf().setAppName("Ophbase2").setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val tableName = "airline_day_search_a"
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
      hcd.setMinVersions(0)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }

    //创建表连接
    val table=new HTable(configuration,TableName.valueOf(tableName));
    //将数据自动提交功能关闭
    table.setAutoFlush(true);
    //设置数据缓存区域
    table.setWriteBufferSize(64*1024*1024);
    //然后开始写入数据
    var num=0;
    while(num<100) {
      var rrrd = sqlContext.table("sp_class.flight_route").map(r => {
        val scode = r.getAs[String]("scode")
        val ecode = r.getAs[String]("ecode")
        var md5hash = md5Hash(scode + ecode)
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("MMdd")
        var cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -(new java.util.Random).nextInt(100))
        var yesterday = dateFormat.format(cal.getTime())
        val key = md5hash.substring(0, 3) + scode + ecode + yesterday + rand3Str()
        var random = (new java.util.Random).nextInt(15)
        if (random < 3) random = 3;
        var j = 1;
        var vp = v
        while (j <= random) {
          vp = vp + "," + v
          j = j + 1
        }
        vp = "{\"fl\": [" + vp + "]}"
        //(key, Seq(vp))
        (key, vp)
      })
      val rrdArr = rrrd.collect()
      val arrLen = rrdArr.length
      var i = 0;
      val itor = rrdArr.array
      var putList:java.util.List[Put] = new util.ArrayList[Put]();
      while (i < 24) {
        val k = itor.length
        var j = 0
        while (j < k) {
          println(itor(j)._1 + "    \t" + i)
          var put = new Put(Bytes.toBytes(itor(j)._1))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("linedata"), i, Bytes.toBytes(itor(j)._2))
          //table.put(put)
          putList.add(put)
          j = j + 1
        }
        i = i + 1
      }
      table.put(putList)   //批量插入:put(List<Put> list)
      num=num+1
    }
    //刷新缓存区
    //table.flushCommits();
    //关闭表连接
    table.close();
    sc.stop()
  }
}
