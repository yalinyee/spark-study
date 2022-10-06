package com.study.spark.core.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkJoinTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Join Test:会产生笛卡尔乘积!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2)
    ),2)

    val rdd2 = sc.makeRDD(List(
      ("c",6),("a",4),("b",5)
    ),2)

    // Key不同 则对应数据无法连接
    // Key重复 则对应数据多次连接(会产生笛卡尔乘积)

    val sortByKeydd  = rdd1.join(rdd2)

    println(sortByKeydd.collect().mkString(","))
    sc.stop()
  }
}
