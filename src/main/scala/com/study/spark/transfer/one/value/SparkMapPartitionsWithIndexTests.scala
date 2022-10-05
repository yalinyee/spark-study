package com.study.spark.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkMapPartitionsWithIndexTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" MapPartitionsWithIndex Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

//    val mapRdd = rdd.mapPartitionsWithIndex((index,iter) =>{
//      List((index,iter.max)).iterator
//    })

    val mapRdd = rdd.mapPartitionsWithIndex((index,iter) =>
    {
      if(index == 1)
      {
        iter
      }
      else
      {
        Nil.iterator
      }

    })
    println(mapRdd.collect().mkString(","))
    sc.stop()
  }
}
