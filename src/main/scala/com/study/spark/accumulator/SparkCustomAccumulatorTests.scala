package com.study.spark.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkCustomAccumulatorTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" CustomAccumulator Test: 自定义累加器!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      "hello scala","hello","hello spark","hello java"
    ))

    //创建累加器
    val acc = new MyWordCountAccumulator

    //注册累加器
    sc.register(acc)

    //使用累加器
    rdd.flatMap(_.split(" ")).foreach{
      word => {
        acc.add(word)
      }
    }

    //获取累加器的值
    println(acc.value)

    sc.stop()
  }

  // 自定义累加器
  class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String,Int]]{

    //存储wordCount集合
    private var wordCountMap = mutable.Map[String,Int] ()


    /**
     * 功能描述 初始值
     *
     * @return
     */
    override def isZero: Boolean =
    {
      wordCountMap.isEmpty
    }

    /**
     * 功能描述  复制累加器
     *
     * @param
     * @return
     */
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] =
    {
      new MyWordCountAccumulator
    }

    /**
     * 功能描述 重置累加器: 创建累加器后清空累加器
     *
     * @param
     * @return
     */
    override def reset(): Unit =
    {
      wordCountMap.clear()
    }

    override def add(word: String): Unit =
    {
      wordCountMap(word) = wordCountMap.getOrElse(word,0) + 1
    }

    /**
     * 功能描述  合并当前累加器和其他累加器 -- Driver端合并累加器
     *
     * @param   other  其他累加器
     * @return
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit =
    {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap = map1.foldLeft(map2)((map,kv) =>
        {
          map(kv._1) = map.getOrElse(kv._1,0) + kv._2

          map
        })
    }

    override def value: mutable.Map[String, Int] =
    {
      wordCountMap
    }
  }
}
