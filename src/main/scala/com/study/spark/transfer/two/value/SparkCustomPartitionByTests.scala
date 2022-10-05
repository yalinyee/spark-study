package com.study.spark.transfer.two.value

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SparkCustomPartitionByTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Custom Partition Test: 自定义分区规则   !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("aaa",1),("bbb",2),("ccc",3)
    ),3)

    // partitionBy来自于RDD 半生对象中的隐式转换  -->  k,v -> PairRDDFunctions
    val partitionRdd = rdd1.partitionBy(new MyPartitioner(3))

    val mapRdd = partitionRdd.mapPartitionsWithIndex((index,datas) =>
    {
      datas.map(data => (index,data))
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }

  class MyPartitioner(num: Int) extends Partitioner {
    override def numPartitions: Int = {
      num
    }

    override def getPartition(key: Any): Int = {
      key match {
        case "aaa" => 0
        case _ => 1
      }
    }
  }
}

