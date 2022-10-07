package com.study.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 功能描述 通过SparkStreaming从Kafka读取数据
 *
 * @param
 * @return
 */
object SparkKafkaTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("通过SparkStreaming从Kafka读取数据")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))


    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,localhost:9093,localhost:9094",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-stream",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
        KafkaUtils.createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](Set("spark-study"), kafkaPara)
      )

    val ds = kafkaDStream.map(record => record.value())

    //计算WordCount
    ds.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    // 启动采集器
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }


}
