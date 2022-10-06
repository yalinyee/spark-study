package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkFirstTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" First Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),1)

    val result = rdd.first()

    println(result )


    sc.stop()
  }
}
