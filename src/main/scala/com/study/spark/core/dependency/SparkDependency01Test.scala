package com.study.spark.core.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkDependency01Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Dependency Test: 血缘关系(完整依赖关系)!").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.makeRDD(List(
      ("Hello java"),("Hello spark")
    ))
    println(fileRDD.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)

    resultRDD.collect()

    sc.stop()
  }
}
