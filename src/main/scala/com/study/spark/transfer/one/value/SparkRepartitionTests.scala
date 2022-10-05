package com.study.spark.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkRepartitionTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Repartition Test: 重分区 !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,1,1,2,2,2),2)
    println("分区数:" +  rdd.getNumPartitions)

    val mapRdd = rdd.filter( _ % 2 ==0).repartition(6)

    println("分区数:" +  mapRdd.getNumPartitions)

    sc.stop()
  }
}
