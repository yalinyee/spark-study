package com.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 功能描述
 *
 * @param
 * @return
 */
object SparkQueueTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Queue: 模拟数据生成")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))

    val queue = new mutable.Queue[RDD[Int]]

    val queueDs: InputDStream[Int] = ssc.queueStream(queue)

    queueDs.print()

    // 启动采集器
    ssc.start()

    // 往queue不断加内容
    for(i <- 1 to 6) {
      val rdd = ssc.sparkContext.makeRDD(List(i))
      queue.enqueue(rdd)
      Thread.sleep(1000)
    }

    // 等待采集器结束
    ssc.awaitTermination()
  }

}
