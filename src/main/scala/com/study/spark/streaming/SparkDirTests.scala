package com.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 功能描述
 *
 * @param
 * @return
 */
object SparkDirTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("目录采集数据")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))


    val dirDs: DStream[String] = ssc.textFileStream("in")

    dirDs.print()

    // 启动采集器
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }

}
