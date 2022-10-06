package com.study.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Memory {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Spark Memory Create")

    // 创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(sparkConf)

    val list = List(1,2,3,4)
    val rdd = sc.parallelize(list)
    rdd.collect().foreach(println)

    val makeRdd = sc.makeRDD(list)
    println(makeRdd.collect().mkString(","))

    sc.stop()
  }
}
