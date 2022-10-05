package com.study.spark.persist

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkPersistTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Persist Test: RDD缓存数据   !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD = rdd.map(num =>
    {
      println("map ************* ")
     num
    })

    // 将结果缓存起来,默认缓存在计算节点Executor内存中
    // cache底层调用的是persist方法
    val cacheRdd = mapRDD.cache()

    println(cacheRdd.toDebugString)

    println(cacheRdd.collect().mkString(","))
    println("=========================")
    println(cacheRdd.toDebugString)
    val result = cacheRdd.reduce(_ + _)

    println("reduce job result: " + result)

    sc.stop()
  }
}
