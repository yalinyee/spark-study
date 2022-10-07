package com.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * 功能描述
 *
 * @param
 * @return
 */
object SparkWordCountTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming WordCount")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))

    // 从socket获取数据,一行一行的获取数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream( "localhost", 8888)

    val wordDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    val wordToSumDS: DStream[(String,Int)] = wordDStream.map((_,1)).reduceByKey(_ + _)

    wordToSumDS.print()

    // 在window下打开netcat
    // cmd中执行命令:  nc -lp 8888 发送数据

    // Driver程序执行过程中不能结束
    // 采集器在正常情况下启动后不能停止,除非特殊情况
    // ssc.stop()

    // 启动采集器
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }

}
