package com.study.spark.action

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkReduceTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Reduce Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),1)

    val result = rdd.reduce(_ + _)

    println(result)


    sc.stop()
  }
}
