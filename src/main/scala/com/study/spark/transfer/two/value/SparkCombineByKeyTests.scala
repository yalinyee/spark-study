package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkCombineByKeyTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" CombineByKey Test:!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    // 求每个Key的平均值：相同Key总和 / 相同Key数量
    val rdd = sc.makeRDD(List(
      ("a",88),("b",95),("a",91),
      ("b",93),("a",95),("b",98)
    ),2)

    // zeroValue :  分区内计算初始值,只分区内计算用到
    // seqOp     :  分区内计算规则,相同Key的value计算
    // combOp    :  分区间计算规则,相同Key的value计算
    //如果分区内和分区间的计算规则都相同,就相当于 foldByKey
    //    88 -> (88,1) --> (88,1) +  91  --->(179,2)
    //计算时需要将 value 格式发生改变,只需要第一个 value 发生改变即可

    // createCombiner : 将第一个value改变结构
    // mergeValue     : 分区内的计算规则
    // mergeCombiners : 分区间的计算规则
    val combineRdd = rdd.combineByKey(createCombiner = v =>(v, 1), mergeValue = (t: (Int,Int), v) => (t._1 + v, t._2 + 1), mergeCombiners= (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2 ))

    val resultRdd = combineRdd.map{
      case (key,(total,cnt)) => (key,total / cnt)
    }

   // 相当于以下计算
//    val resultRdd = rdd.groupByKey().map{
//      case (key,iter) => {
//        (key,(iter.sum,iter.size))
//      }
//    }.map{
//      case (key,(total,cnt))  => (key,total / cnt)
//    }

    println(resultRdd.collect().mkString(","))

    sc.stop()
  }
}
