package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkAggregateTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Aggregate Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //aggregateByKey 初始值只参与分区内计算
    //aggregate      初始值参与分区内和分区间的计算
    val result = rdd.aggregate(2)(_ + _, _ * _)

    println(result)


    sc.stop()
  }
}
