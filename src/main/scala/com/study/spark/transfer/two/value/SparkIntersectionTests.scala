package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkIntersectionTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Intersection Test: 差集 !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1,2,3,4),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)

    //并集：数据合并，分区合并
    val unionRdd = rdd1.union(rdd2)
    println(unionRdd.collect().mkString(","))

    //交集
    var interRdd = rdd1.intersection(rdd2)
    println(interRdd.collect().mkString(","))

   //差集
    val subtractRdd = rdd1.subtract(rdd2)
    println(subtractRdd.collect().mkString(","))
   //拉链
   val zipRdd = rdd1.zip(rdd2)
    println(zipRdd.collect().mkString(","))

    sc.stop()
  }
}
