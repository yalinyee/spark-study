package com.study.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 功能描述 通过SparkStreaming从Kafka读取数据
 *
 * @param
 * @return
 */
object SparkUpdateStateByKeyTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("有状态转化操作：spark每个采集周期保存计算结果,以便后续采集周期处理")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.sparkContext.setCheckpointDir("cp")

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
          ConsumerStrategies.Subscribe[String, String](Set("spark-transform"), kafkaPara)
      )

    // updateStateByKey有状态计算函数：spark每个采集周期保存计算结果,以便后续采集周期处理
    // updateStateByKey: 第一个参数表示相同的Key的value集合
    // updateStateByKey: 第二个参数表示表示相同的Key缓冲区数据,有可能为空
    // 计算的中间结果需要保存到检查点的位置中,需要设置检查点位置
    val ds: DStream[String] = kafkaDStream.map(rdd => rdd.value())
    ds.flatMap(_.split(" "))
      .map((_,1L))
//      .reduceByKey(_ + _)
      .updateStateByKey[Long](
        (seq: Seq[Long],buffer: Option[Long]) =>
        {
          val newBufferValue = buffer.getOrElse(0L) + seq.sum

          Option(newBufferValue)
        }
      ).print()

    // 启动采集器
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }


}
