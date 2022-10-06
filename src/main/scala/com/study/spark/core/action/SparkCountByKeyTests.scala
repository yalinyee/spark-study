package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkCountByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" CountByKey Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)

    val result = rdd.countByKey()

    println(result)


    sc.stop()
  }
}
