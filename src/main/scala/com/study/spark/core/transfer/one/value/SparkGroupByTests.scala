package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkGroupByTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Groub by Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRdd = rdd.groupBy(num =>
    {
      num % 2
    })

    mapRdd.collect().foreach({
      case (key,list) =>
      {
          println("key: " + key + ",list:" + list.mkString(","))
      }
    })

    sc.stop()
  }
}
