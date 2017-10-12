package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.stu.hbase.CurSearch


class SearchAnalyze() extends Serializable with HBaseWriteSupport
  with DefaultWrites
  with HBaseReadSupport
  with DefaultReads
  with HBaseDeleteSupport
  with HFileSupport
  with HBaseUtils {

  def train(sc: SparkContext, data: TrainingData): RDD[(String, Array[((String, String), String, String, Double)])] = {
    val ratings: RDD[Rating] = data.ratings
    val citymap = Map[String, String]("SHS_C" -> "SHA", "BJS_C" -> "PEK", "CQS_C" -> "CKG");
    var rr = ratings.groupBy(r => r.user).map(R => {
      val user = R._1;
      var in_rdd = R._2.map(r => {
        var sig = (dateformat.parse(curDate).getTime - dateformat.parse(r.dt).getTime) / (1000 * 3600 * 24)
        val sigre = Math.pow(sigmod, sig)
        var dep = r.dep;
        var arr = r.arr;
        if (citymap.contains(r.dep)) {
          dep = citymap.get(r.dep).get
        }
        if (citymap.contains(r.arr)) {
          arr = citymap.get(r.arr).get
        }
        Row(dep, arr, r.cabin, r.depdate, sigre)
      })
      var rate_rdd = in_rdd.groupBy(r => (r.getString(0), r.getString(1))).map(x => {
        val iter: Iterator[Row] = x._2.iterator;
        var rate = 0.0
        var deparr: Array[String] = Array[String]();
        var cabin_tmp: String = null;
        while (iter.hasNext) {
          var r = iter.next()
          rate = rate + r.getAs[Double](4);
          //depdate_tmp = r.getString(3)
          deparr = deparr :+ r.getString(3)
          cabin_tmp = r.getString(2)
        }
        (x._1, deparr.sorted.take(deparr.length / 2 + 1).takeRight(1).head, cabin_tmp, rate)
      }).toArray
      var toparr = rate_rdd.sortBy(f => f._4).takeRight(6)
      (user, toparr)
    })
    rr
  }

  def curDate: String = dateformat.format(Calendar.getInstance().getTime)

  def dateformat = new SimpleDateFormat("yyyy-MM-dd")

  def sigmod: Double = 0.7

  def save(sc: SparkContext, rdd: RDD[(String, Array[((String, String), String, String, Double)])], ssqlcontext: SQLContext): Unit = {

    val tableName = "s_user_search"
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.rootdir", "hdfs://iZ2ze0jk5poqsx70to99qtZ:8020/hbase")
    configuration.setBoolean("hbase.cluster.distributed", true)
    configuration.set("hbase.zookeeper.quorum", "10.0.1.190,10.0.1.188,10.0.1.189")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    //val conn = ConnectionFactory.createConnection(conf)
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(configuration)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("basic"))
      admin.createTable(tableDesc)
    }

    /*val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/hbase/data/default/s_user_search")
    jobConfig.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])

    jobConfig.set(org.apache.hadoop.hbase.mapred.TableOutputFormat.OUTPUT_TABLE, tableName)*/
    /*test var rrr=rdd.map(r=>Row(r._1,r._2.mkString("#")))
    val schema_b = StructType(
      StructField("userid", StringType) ::
        StructField("dep", StringType)::Nil);
    ssqlcontext.createDataFrame(rrr, schema_b).saveAsTable("test_search_ssss")*/

    val retRDD = rdd.map(r => {
      val rd = r._2.map(X => {
        //(r._1, X._1._1,X._1._2,  X._2,X._3,X._4)
        //new StringBuffer("{'dep':'").append(X._1._1).append("','arr':'").append(X._1._2).append("','date':'").append(X._2).append("','cabin':'").append(X._3).append("','rate':").append(X._4).append("}").toString
        (X._1._1, X._1._2, X._2, X._3, X._4)
      })
      val offlinedata = r._2.map(X => {
        new StringBuffer("{'dep':'").append(X._1._1).append("','arr':'").append(X._1._2).append("','date':'").append(X._2).append("','cabin':'").append(X._3).append("','rate':").append(X._4).append("}").toString
      })

      var R = r._2.sortBy(X => X._4).takeRight(1).head;

      (r._1, R._1._1, R._1._2, R._2, R._3, R._4, rd.mkString("#"), "[" + offlinedata.mkString(",") + "]")
    })

    ssqlcontext.sql("drop table test_s_user_search")
    val rrdtest = retRDD.map(X => Row(X._1, X._2, X._3, X._4, X._5, X._6))
    val schema = StructType(
      StructField("userid", StringType) ::
        StructField("dep", StringType) ::
        StructField("arr", StringType) ::
        StructField("depdate", StringType) ::
        StructField("cabin", StringType) ::
        StructField("rate", DoubleType)
        :: Nil);
    var resultdf = ssqlcontext.createDataFrame(rrdtest, schema);
    resultdf.saveAsTable("test_s_user_search");

    var rrrd = retRDD.map(X => (X._1, Seq(X._2, X._3, X._4, X._5, X._6.toString, X._7, X._8)))
    val numCols = 5
    val cols = Seq("dep", "arr", "depdate", "cabin", "rate", "data", "offline").to[Seq]
    // must be a collection.Seq and not a collection.immutable.Seq
    implicit val conf = HBaseConfig(configuration)
    toHFileRDDFixed(rrrd).toHBaseBulk(tableName, "basic", cols)
    //val resRDD = retRDD.map(convert);
    /*new PairRDDFunctions(resRDD)*/
    //resRDD.saveAsNewAPIHadoopDataset(jobConfig)
  }

  def saveSec(sc: SparkContext, rdd: RDD[(String, Array[((String, String), String, String, Double)])]): Unit = {
    val tablename = "s_user_search_b"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "10.0.1.188,10.0.1.189")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("K1,jack,15", "K2,Lily,16", "K3,mike,16"))
    val retRDD = rdd.map(r => {
      val rd = r._2.map(X => {
        (r._1, X._1._1, X._1._2, X._2, X._3, X._4)
      })
      rd
    }).flatMap(x => x)
    val resRDD = retRDD.map(convert);

    resRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }

  def convert(triple: (String, String, String, String, String, Double)): (ImmutableBytesWritable, Put) = {

    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("dep"), Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("arr"), Bytes.toBytes(triple._3))
    p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("depdate"), Bytes.toBytes(triple._4))
    p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("cabin"), Bytes.toBytes(triple._5))
    p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("rate"), Bytes.toBytes(triple._6))
    (new ImmutableBytesWritable, p)
  }

  def readfun(sc: SparkContext): Unit = {
    val tablename = "s_user_search"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "10.0.1.188,10.0.1.189")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)

    hBaseRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val dep = Bytes.toString(result.getValue("basic".getBytes, "dep".getBytes))
      val arr = Bytes.toString(result.getValue("basic".getBytes, "arr".getBytes))
      val rate = Bytes.toString(result.getValue("basic".getBytes, "rate".getBytes))

      println("Row key:" + key + " Dep:" + dep + " Arr:" + arr + "rate:" + rate)
    }
    }

    sc.stop()
    admin.close()
  }
}

object SearchAnalyze {
  private var ssqlcontext: SQLContext = null;
  private var sc: SparkContext = null;

  def main(args: Array[String]) {
    ssqlcontext = getSingleContext()
    System.setProperty("HADOOP_USER_NAME", "hive")
    /*val ds = new DataSource()
    val data = ds.readTraining(sc, ssqlcontext)

    val inst = new SearchAnalyze()
    val rdd = inst.train(sc, data)
    inst.save(sc, rdd, ssqlcontext)*/
    val eval_inst = new Evaluation()
    eval_inst.evaluate(sc,ssqlcontext)

    //inst.readfun(sc)
  }

  def getSingleContext(): SQLContext = {
    if (ssqlcontext == null) {
      ssqlcontext = getSqlContext();
    }
    ssqlcontext
  }

  private def getSqlContext(): SQLContext = {
    val conf = new SparkConf().setAppName("SearchAnalyze").setMaster("yarn-client")
    sc = new SparkContext(conf)
    sc.setLocalProperty("spark.scheduler.pool", "myfair")
    val sqlContext = new HiveContext(sc)
    sqlContext
  }

}

class DataSource {

  def readTraining(sc: SparkContext, ssqlcontext: SQLContext): TrainingData = {
    new TrainingData(getRatings(sc, ssqlcontext))
  }

  def getRatings(sc: SparkContext, ssqlcontext: SQLContext): RDD[Rating] = {
    val df = ssqlcontext.sql("select userid,dep,arr,searchcabincode,pdate,dt  from apilog.api_airticket_search where dt>=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),15)  and  dt<from_unixtime(unix_timestamp(),'yyyy-MM-dd') and (searchid='NULL' or usecache=1 ) and userid!='0' and pdate>=CURRENT_DATE() ")
    //ssqlcontext.sql("drop table test_s_search_src")
    //df.saveAsTable("test_s_search_src")
    var rdd = df.map(r => {
      val userid = r.getAs[String]("userid");
      val dep = r.getAs[String]("dep");
      val arr = r.getAs[String]("arr");
      val cabin = r.getAs[String]("searchcabincode");
      val pdate = r.getAs[String]("pdate");
      val dt = r.getAs[String]("dt");
      Rating(user = userid, dep = dep, arr = arr, cabin = cabin, depdate = pdate, dt = dt)
    })
    rdd
  }

}

class Evaluation extends Serializable {

  val citymap = Map[String, String]("SHS_C" -> "SHA", "BJS_C" -> "PEK", "CQS_C" -> "CKG");

  def evaluate(sc: SparkContext, ssqlcontext: SQLContext): Unit = {

    val sql = "select user_id,dep_code,arr_code,dep_time,cabin,d.create_time from  s_airline_order_detail d join s_airline_order o on (o.order_id=d.order_id and o.orderstatue in ('5', '52', '53', '71', '73', '74', '75') and o.dt>=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) and d.dt>=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) and dep_time>=CURRENT_DATE() )"
    val payDF = ssqlcontext.sql(sql)

    val predictRdd = payDF.rdd.keyBy[String](_.getAs[String]("user_id")).groupByKey().map(X => {
      val itor: Iterator[Row] = X._2.toIterator
      val predictArr: Array[(String, String, String, String, Double)] = CurSearch.getUserComposite(X._1)
      var num: Int = 0;
      var ticketSize = 0
      var compose: Array[(String, String)] = Array[(String, String)]();
      while (itor.hasNext) {
        val row = itor.next()
        var dep = row.getString(1)
        var arr = row.getAs[String](2)
        if (citymap.contains(dep)) {
          dep = citymap.get(dep).get
        }
        if (citymap.contains(arr)) {
          arr = citymap.get(arr).get
        }
        var it = predictArr.iterator
        var flag: Boolean = false
        while (it.hasNext) {
          val temp = it.next()
          if (temp._1 == dep && temp._2 == arr) {
            flag = true;
          }
        }
        compose = compose :+ (dep, arr);
        if (flag) {
          num = num + 1
        }
        ticketSize = ticketSize + 1;
      }
      Row(X._1, num, ticketSize, compose, predictArr.mkString("#"))
    })
    val schema = StructType(
      StructField("userid", StringType) ::
        StructField("num", IntegerType) ::
        StructField("ticketsize", IntegerType) ::
        StructField("tickets", ArrayType(StructType(Array(
          StructField("dep", StringType),
          StructField("arr", StringType)
        )))) ::
        StructField("predict", StringType)
        :: Nil);
    var resultdf = ssqlcontext.createDataFrame(predictRdd, schema);
    ssqlcontext.sql("drop table tmp.userticket_predict")
    resultdf.saveAsTable("tmp.userticket_predict")
  }
}

case class Rating(
                   user: String,
                   dep: String,
                   arr: String,
                   cabin: String,
                   depdate: String,
                   dt: String
                 )

class TrainingData(
                    val ratings: RDD[Rating]
                  ) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}

