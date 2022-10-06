package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkGlomTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Glom Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val nums = rdd.getNumPartitions
    println(nums)


    val mapRdd = rdd.glom()

    mapRdd.collect().foreach(array =>
    {
      println(array.mkString(","))
    })

    sc.stop()
  }
}
