package com.study.spark.practice

import org.apache.spark.{SparkConf, SparkContext}

object Practice1Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" 统计出每一个省份每个广告被点击数量排行的Top3").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    val rdd = sc.textFile("input/agent.log")

    val resultRdd = rdd.map(attr =>
    {
      val splitStr = attr.split(" ")
      ((splitStr(1),splitStr(4)),1)
    }).reduceByKey(_ + _)
      .map(attr => (attr._1._1,(attr._1._2,attr._2)))
      .groupByKey.mapValues(item =>
    {
      item.toList.sortWith((left,right) =>
      {
        left._2 > right._2
      }).take(3)
    })


    resultRdd.collect().foreach(println)
    sc.stop()
  }
}
