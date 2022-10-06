package com.study.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkForeachTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Foreach Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // rdd.collect().foreach 就是方法,集合的方法就是在当前节点(Driver)执行的
    rdd.collect().foreach(println)
    println("****************************")

    //rdd.foreach 就是一个算子
    //rdd的方法称为算子
    //算子的逻辑代码在分布式计算节点Executor执行的
    //算子之外的代码是在Driver端执行的
    rdd.foreach(println)


    sc.stop()
  }
}
