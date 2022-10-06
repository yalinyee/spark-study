package com.study.spark.core.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkClosureCleanerTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" ClosureCleaner Test: 闭包序列化检测!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List())

    //Exception : Task not serializable
    //如果算子中使用了外的对象(闭包),那么在执行时需要序列化
    val user = new User()
    rdd.foreach(num =>
    {
      println("age: " + (user.age + num))
    })


    sc.stop()
  }

  class User   {
    val age :Int = 20
  }
}
