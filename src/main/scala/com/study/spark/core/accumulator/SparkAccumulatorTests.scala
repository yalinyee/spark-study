package com.study.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object SparkAccumulatorTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Accumulator Test: 累加器!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ))

    //累加器: 分布式共享只写变量
    val sum = sc.longAccumulator("sum累加器")
    rdd.foreach{
      case (_,count) =>{
        sum.add(count)
      }
    }

    println("a:" + sum.value)
    sc.stop()
  }
}
