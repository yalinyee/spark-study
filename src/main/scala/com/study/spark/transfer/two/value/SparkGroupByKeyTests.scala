package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkGroupByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" GroupByKey Test: 面向整个数据集!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("hello",1),("java",2),("hello",3),("scala",1),("spark",2)
    ),1)

    val groupBydd  = rdd.groupByKey()

    val wordCountRdd = groupBydd.map{
      case (key,iter) =>
      {
        (key,iter.sum)
      }
    }

    println(wordCountRdd.collect().mkString(","))
    sc.stop()
  }
}
