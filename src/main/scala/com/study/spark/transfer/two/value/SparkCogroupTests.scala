package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkCogroupTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Cogroup Test:!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3),("c",4)
    ),2)

    val rdd2 = sc.makeRDD(List(
      ("a",4),("b",5),("b",7)
    ),2)

    val resultRdd  = rdd1.cogroup(rdd2)


    resultRdd.collect().foreach(println)
    sc.stop()
  }
}
