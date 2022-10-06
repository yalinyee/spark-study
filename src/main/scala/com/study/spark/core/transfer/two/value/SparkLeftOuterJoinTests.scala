package com.study.spark.core.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkLeftOuterJoinTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" LeftOuterJoin Test:!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3),("a",2)
    ),2)

    val rdd2 = sc.makeRDD(List(
      ("a",4),("b",5)
    ),2)

    val sortByKeydd  = rdd1.leftOuterJoin(rdd2)

//    rdd1.rightOuterJoin(rdd2)

    sortByKeydd.collect().foreach(println)
    sc.stop()
  }
}
