package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkSaveTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Save Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))

     rdd.saveAsTextFile("output1" )
     rdd.saveAsObjectFile("output2")
     rdd.map((_, 1)).saveAsSequenceFile("output3")


    sc.stop()
  }
}
