package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkFlatMapTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" FlatMap Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      List(1,2),List(3,4,5),6,7
    ))

    val mapRdd = rdd.flatMap(list =>
    {
      list match
      {
        case list: List[_] => list
        case d => List(d)
      }

    })
    println(mapRdd.collect().mkString(","))
    sc.stop()
  }
}
