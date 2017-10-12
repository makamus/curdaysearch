package org.stu.hbase

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


object CurSearch {

  val tableName = "search_tickets_s"
  val his_tableName = "s_user_search"
  val sdf = new SimpleDateFormat("MMdd");
  val citymap = Map[String,String]("SHS_C"->"SHA","BJS_C"->"PEK", "CQS_C"->"CKG");

  def getHBaseConn(): HConnection ={
    val conf:Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "10.0.1.190,10.0.1.188,10.0.1.189,10.0.1.78")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val conn = HConnectionManager.createConnection(conf)
    conn
  }

  def getUserCurDay(conn: Connection, phoneid: String): String = {

    var cal: Calendar = Calendar.getInstance()
    val MD = sdf.format(cal.getTime)
    cal.add(Calendar.DAY_OF_YEAR, 1)
    val MD2 = sdf.format(cal.getTime)

    /*//Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)*/

    val userTable: TableName = TableName.valueOf(tableName)

    //获取 user 表
    val table = conn.getTable(userTable)
    var ret: Array[(String, String, String, String, Double)] = Array[(String, String, String, String, Double)]()
    var rev: String = null;
    var predata: String = null;
    var offlinedata:Array[(String,String,String,String,Double)] = Array[(String,String,String,String,Double)]();

    val g = new Get(phoneid.getBytes)
    val histable = conn.getTable(TableName.valueOf(his_tableName))
    try {
      val prerev = histable.get(g)
      predata = Bytes.toString(prerev.getValue("basic".getBytes, "data".getBytes))
      if(predata!=null && predata.length>0) {
        offlinedata = predata.split("#").map(r => {
          val str = r.replace("(", "").replace(")", "").split(",")
          var (dep: String, arr: String, date: String, cabin: String, rate) = (str(0), str(1), str(2), str(3), str(4))
          (dep, arr, date, cabin, rate.toDouble)
        })
      }
    } finally {
      histable.close()
    }

    try {
      //扫描数据
      val s = new Scan()
      s.setBatch(2000)
      s.setStartRow(Bytes.toBytes(phoneid + "_" + MD))
      s.setStopRow(Bytes.toBytes(phoneid + "_" + MD2))
      val scanner = table.getScanner(s)

      try {
        val ite: util.Iterator[Result] = scanner.iterator();
        var arr_data: Seq[(String, String, String, String, Int)] = Seq[(String, String, String, String, Int)]()
        while (ite.hasNext) {
          var r = ite.next()
          var userid = Bytes.toString(r.getValue("basic".getBytes, "userid".getBytes))
          var dep = Bytes.toString(r.getValue("basic".getBytes, "dep".getBytes))
          var arr = Bytes.toString(r.getValue("basic".getBytes, "arr".getBytes))
          var date = Bytes.toString(r.getValue("basic".getBytes, "date".getBytes))
          var searchCabinCode = Bytes.toString(r.getValue("basic".getBytes, "searchCabinCode".getBytes))

          if (citymap.contains(dep)) {
            dep = citymap.get(dep).get
          }
          if (citymap.contains(arr)) {
            arr = citymap.get(arr).get
          }
          arr_data = arr_data :+ (dep, arr, date, searchCabinCode, 1);
        }
        if (arr_data.size > 0) {
          var arrtak = arr_data.groupBy(r => (r._1, r._2)).map(x => {
            x._2.iterator
            val iter: Iterator[(String, String, String, String, Int)] = x._2.iterator;
            var rate = 0.0
            //var depdate_tmp:String = null;
            var deparr: Array[String] = Array[String]();
            var cabin_tmp: String = null;
            while (iter.hasNext) {
              var r = iter.next()
              rate = rate + r._5;
              deparr = deparr :+ r._3
              cabin_tmp = r._4
            }
            (x._1._1, x._1._2, deparr.sorted.take(deparr.length / 2 + 1).takeRight(1).head, cabin_tmp, rate)
          }).toArray
          rev = arrtak.sortBy(f => f._5).takeRight(5).map(X => new StringBuffer("{'dep':'").append(X._1).append("','arr':'").append(X._2).append("','date':'").append(X._3).append("','cabin':'").append(X._4).append("','rate':").append(X._5).append("}").toString).mkString(",")
          offlinedata ++= arrtak
          offlinedata = offlinedata.groupBy(r => (r._1, r._2)).map(x => {
            val iter: Iterator[(String, String, String, String, Double)] = x._2.iterator;
            var rate = 0.0
            //var depdate_tmp:String = null;
            var deparr: Array[String] = Array[String]();
            var cabin_tmp: String = null;
            while (iter.hasNext) {
              var r = iter.next()
              rate = rate + r._5;
              deparr = deparr :+ r._3
              cabin_tmp = r._4
            }
            (x._1._1, x._1._2, deparr.sorted.take(deparr.length / 2 + 1).takeRight(1).head, cabin_tmp, rate)
          }).toArray
          rev = "[" + rev + "]"
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        //确保scanner关闭
        scanner.close()
      }
    } finally {
      table.close()
    }
    if(offlinedata.size>0) {
      predata = offlinedata.sortBy(f => f._5).takeRight(5).map(X => new StringBuffer("{'dep':'").append(X._1).append("','arr':'").append(X._2).append("','date':'").append(X._3).append("','cabin':'").append(X._4).append("','rate':").append(X._5).append("}").toString).mkString(",")
      predata = "[" + predata + "]"
    }

    "{'curday':" + rev + ",'compose':" + predata + "}"
  }

  def getUserComposite( phoneid: String): Array[(String,String,String,String,Double)]  = {
    val conf:Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "10.0.1.190,10.0.1.188,10.0.1.189,10.0.1.78")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    var cal: Calendar = Calendar.getInstance()
    val MD = sdf.format(cal.getTime)
    cal.add(Calendar.DAY_OF_YEAR, 1)
    val MD2 = sdf.format(cal.getTime)

    /*//Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)*/

    val userTable: TableName = TableName.valueOf(tableName)

    //获取 user 表
    val table = conn.getTable(userTable)
    var ret: Array[(String, String, String, String, Double)] = Array[(String, String, String, String, Double)]()
    var rev: String = null;
    var predata: String = null;
    var offlinedata:Array[(String,String,String,String,Double)] = Array[(String,String,String,String,Double)]();

    val g = new Get(phoneid.getBytes)
    val histable = conn.getTable(TableName.valueOf(his_tableName))
    try {
      val prerev = histable.get(g)
      predata = Bytes.toString(prerev.getValue("basic".getBytes, "data".getBytes))
      if(predata!=null && predata.length>0) {
        offlinedata = predata.split("#").map(r => {
          val str = r.replace("(", "").replace(")", "").split(",")
          var (dep: String, arr: String, date: String, cabin: String, rate) = (str(0), str(1), str(2), str(3), str(4))
          (dep, arr, date, cabin, rate.toDouble)
        })
      }
    } finally {
      histable.close()
    }

    try {
      //扫描数据
      val s = new Scan()
      s.setBatch(2000)
      s.setStartRow(Bytes.toBytes(phoneid + "_" + MD))
      s.setStopRow(Bytes.toBytes(phoneid + "_" + MD2))
      val scanner = table.getScanner(s)

      try {
        val ite: util.Iterator[Result] = scanner.iterator();
        var arr_data: Seq[(String, String, String, String, Int)] = Seq[(String, String, String, String, Int)]()
        while (ite.hasNext) {
          var r = ite.next()
          var userid = Bytes.toString(r.getValue("basic".getBytes, "userid".getBytes))
          var dep = Bytes.toString(r.getValue("basic".getBytes, "dep".getBytes))
          var arr = Bytes.toString(r.getValue("basic".getBytes, "arr".getBytes))
          var date = Bytes.toString(r.getValue("basic".getBytes, "date".getBytes))
          var searchCabinCode = Bytes.toString(r.getValue("basic".getBytes, "searchCabinCode".getBytes))

          if (citymap.contains(dep)) {
            dep = citymap.get(dep).get
          }
          if (citymap.contains(arr)) {
            arr = citymap.get(arr).get
          }
          arr_data = arr_data :+ (dep, arr, date, searchCabinCode, 1);
        }
        if (arr_data.size > 0) {
          var arrtak = arr_data.groupBy(r => (r._1, r._2)).map(x => {
            x._2.iterator
            val iter: Iterator[(String, String, String, String, Int)] = x._2.iterator;
            var rate = 0.0
            //var depdate_tmp:String = null;
            var deparr: Array[String] = Array[String]();
            var cabin_tmp: String = null;
            while (iter.hasNext) {
              var r = iter.next()
              rate = rate + r._5;
              deparr = deparr :+ r._3
              cabin_tmp = r._4
            }
            (x._1._1, x._1._2, deparr.sorted.take(deparr.length / 2 + 1).takeRight(1).head, cabin_tmp, rate)
          }).toArray
          rev = arrtak.sortBy(f => f._5).takeRight(5).map(X => new StringBuffer("{'dep':'").append(X._1).append("','arr':'").append(X._2).append("','date':'").append(X._3).append("','cabin':'").append(X._4).append("','rate':").append(X._5).append("}").toString).mkString(",")
          offlinedata ++= arrtak
          offlinedata = offlinedata.groupBy(r => (r._1, r._2)).map(x => {
            val iter: Iterator[(String, String, String, String, Double)] = x._2.iterator;
            var rate = 0.0
            //var depdate_tmp:String = null;
            var deparr: Array[String] = Array[String]();
            var cabin_tmp: String = null;
            while (iter.hasNext) {
              var r = iter.next()
              rate = rate + r._5;
              deparr = deparr :+ r._3
              cabin_tmp = r._4
            }
            (x._1._1, x._1._2, deparr.sorted.take(deparr.length / 2 + 1).takeRight(1).head, cabin_tmp, rate)
          }).toArray
          rev = "[" + rev + "]"
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        //确保scanner关闭
        scanner.close()
      }
    } finally {
      table.close()
      conn.close()
    }
    if(offlinedata.size>0) {
      offlinedata = offlinedata.sortBy(f => f._5).takeRight(5)
    }

    offlinedata
  }

  def main(args: Array[String]): Unit = {
    println(sdf.format(new java.util.Date(1505896992000L)))
    val conn=getHBaseConn()
    var d1=System.currentTimeMillis()
    var ret = getUserCurDay(conn,"bDIFGs+NV2/pRKugX208cQ==")
    println("=="+(System.currentTimeMillis()-d1) )
    println(ret.mkString("#"))
    d1=System.currentTimeMillis()
    ret = getUserCurDay(conn,"xxD/ojlzt4npRKugX208cQ==")
    println("=="+(System.currentTimeMillis()-d1) )
    println(ret.mkString("#"))
    conn.close()
  }
}
