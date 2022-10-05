package com.study.spark.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkSortByTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" SortBy Test: 排序 !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,3,4,2),2)

    val mapRdd = rdd.sortBy(num =>num,false)

    println(mapRdd.collect().mkString(","))

    sc.stop()
  }
}
