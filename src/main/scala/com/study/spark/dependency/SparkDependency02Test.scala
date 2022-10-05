package com.study.spark.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDependency02Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Dependency Test: RDD依赖关系!").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.makeRDD(List(
      ("Hello java"),("Hello spark")
    ))
    println(fileRDD.dependencies)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)

    resultRDD.collect()


    sc.stop()
  }
}
