package org.stu.hbase

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future}

/**
  * Created by lxb on 12/29/2017.
  */

class OperationWriteHbase(val threadId:Int=0) extends Runnable  {
  override def run(): Unit = {

  }
}


object MultiThreadOphbase {

  def main(args: Array[String]): Unit = {
    val poolsize = 10 ;
    val pool: ExecutorService = Executors.newFixedThreadPool(poolsize)
    var i = 0;
    var lst = new util.ArrayList[Future[_]]()
    while(i<poolsize){
      val future:Future[_] =pool.submit( new OperationWriteHbase( (i) )   )
      lst.add(future)
      i=i+1;
    }
    i=0;
    while(i<lst.size()){
      try {
        lst.get(i)
        i = i+1;
      }catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
