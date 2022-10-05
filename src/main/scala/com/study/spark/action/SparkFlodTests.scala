package com.study.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkFlodTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Flod Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //aggregateByKey 初始值只参与分区内计算
    //aggregate      初始值参与分区内和分区间的计算
    val result = rdd.fold(2)(_ + _)

    println(result)


    sc.stop()
  }
}
