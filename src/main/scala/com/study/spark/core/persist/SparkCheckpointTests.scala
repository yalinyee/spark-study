package com.study.spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

object SparkCheckpointTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Checkpoint Test: 将RDD中间结果写入磁盘!").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cpDir")

    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD = rdd.map(num =>
    {
      println("map ************* ")
     num
    })

    // 将结果保存到分布式文件系统中,会启动新的JOB,再执行一次保存重要数据。
    // 为了提高性能，检查点操作会和cache联合使用
    val cacheRdd = mapRDD.cache()

    //checkpoint会切断血缘关系,一旦数据丢失,不会重头执行
    //因为checkpoint将结果保存到分布式文件系统中,不容易丢失
    //所以会切断血缘关系,等同于生产新的数据源
    cacheRdd.checkpoint()

    println(cacheRdd.collect().mkString(","))
    println("=========================")
    val result = cacheRdd.reduce(_ + _)

    println("reduce job result: " + result)

    sc.stop()
  }
}
