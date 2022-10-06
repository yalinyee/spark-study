package com.study.spark.core.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkSortByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" SortByKey Test:!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)
    ),2)

    val sortByKeydd  = rdd.sortByKey(false)

    println(sortByKeydd.collect().mkString(","))
    sc.stop()
  }
}
