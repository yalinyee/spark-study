package com.study.spark.core.transfer.one.value

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkMapPartitionsTests02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" MapPartitions Test !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),1)

    val mapRdd = rdd.mapPartitions(
      iter => {
        val list = new ListBuffer[Int]
        while(iter.hasNext)
        {
          list += iter.next() * 2
        }
        list.iterator
      }
    )
    println(mapRdd.collect.mkString(","))

    sc.stop()
  }
}
