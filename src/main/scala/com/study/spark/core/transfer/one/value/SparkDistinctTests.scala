package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkDistinctTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Distinct Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3),2)

    val mapRdd = rdd.distinct()
    println(mapRdd.collect().mkString(","))

    sc.stop()
  }
}
