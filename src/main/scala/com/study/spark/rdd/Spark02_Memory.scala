package com.study.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Memory {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02 Memory Create")

    // 创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(sparkConf)

    val list = List(1,2,3,4,5)

    val rdd = sc.makeRDD(list,5)

    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
