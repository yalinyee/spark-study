package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkSampleTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Sample Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    val mapRdd = rdd.sample(false,0.5,2)
    println(mapRdd.collect().mkString(","))
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
