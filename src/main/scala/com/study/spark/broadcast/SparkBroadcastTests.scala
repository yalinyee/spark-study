package com.study.spark.broadcast

import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadcastTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Broadcast Test: 分布式共享只读广播变量!").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3),
    ))
    val list = List(("a",4),("b",5),("c",6))

    //广播变量将数据广播到Executor内存中,避免Task中冗余数据(闭包复制到task)
    val bcList = sc.broadcast(list)

    //(a,(1,4)),(b,(2,5)),(c,(3,6))
    val rdd2 = rdd1.map{
      case (word,count1) => {
        var count2 = 0
        for((k,v) <- bcList.value){
          if(k == word){
            count2 = v
          }
        }
        (word,(count1,count2))
      }
    }

    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
