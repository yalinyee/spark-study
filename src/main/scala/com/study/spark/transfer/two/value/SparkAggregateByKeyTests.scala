package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkAggregateByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" AggregateByKey Test:!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)
    ),2)

    // zeroValue :  分区内计算初始值,只分区内计算用到
    // seqOp     :  分区内计算规则,相同Key的value计算
    // combOp    :  分区间计算规则,相同Key的value计算

    val aggregateByKeydd  = rdd.aggregateByKey(0) (seqOp = (x,y) => x.max(y), combOp = (m,n) => m + n)

    //如果分区内和分区间的计算规则都相同
//    val resultRdd  = rdd.aggregateByKey(0) (seqOp = (x,y) => x + y, combOp = (m,n) => m + n)
//    val resultRdd  = rdd.aggregateByKey(0) (_ +_, _ + _)

    println(aggregateByKeydd.collect().mkString(","))
    sc.stop()
  }
}
