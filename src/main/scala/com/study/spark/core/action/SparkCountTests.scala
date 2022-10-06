package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkCountTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Count Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),1)

    val result = rdd.count()

    println(result)


    sc.stop()
  }
}
