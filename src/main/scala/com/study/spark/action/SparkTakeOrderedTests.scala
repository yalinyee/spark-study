package com.study.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkTakeOrderedTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" TakeOrdered Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(2,1,4,3),1)

    val result = rdd.takeOrdered(3)

    println(result.mkString(","))


    sc.stop()
  }
}
