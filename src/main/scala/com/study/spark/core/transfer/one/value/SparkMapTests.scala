package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkMapTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Map Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val nums = rdd.getNumPartitions
    println(nums)


    val mapRdd1 = rdd.map(attr =>
    {
      println(" map A = " + attr)

      attr
    })


    val mapRdd2 = mapRdd1.map(attr =>
    {
      println(" map B = " + attr)

      attr
    })

    println(mapRdd2.collect().mkString(","))
    sc.stop()
  }
}
