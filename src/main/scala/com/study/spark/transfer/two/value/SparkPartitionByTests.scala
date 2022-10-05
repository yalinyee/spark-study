package com.study.spark.transfer.two.value

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkPartitionByTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" PartitionBy Test: 指定分区规则   !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ),1)

    // partitionBy来自于RDD 半生对象中的隐式转换  -->  k,v -> PairRDDFunctions
    val partitionRdd = rdd1.partitionBy(new HashPartitioner(2))

    println(partitionRdd.getNumPartitions)
    sc.stop()
  }
}
