package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkCollectTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Collect Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),1)

    val result = rdd.collect()

    println(result.mkString(","))


    sc.stop()
  }
}
