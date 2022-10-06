package com.study.spark.core.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkReduceByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" ReduceByKey Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("hello",1),("java",2),("hello",3),("scala",1),("spark",2)
    ),1)

    val reduceRdd  = rdd.reduceByKey(_ + _)

    println(reduceRdd.collect().mkString(","))
    sc.stop()
  }
}
