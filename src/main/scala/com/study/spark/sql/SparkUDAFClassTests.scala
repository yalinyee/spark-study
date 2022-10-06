package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkUDAFClassTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("简单版的用户自定义聚合函数")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //导入隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(List(
      (1,"张三",20),
      (2,"李四",30),
      (3,"王五",40)
    ))

    // 创建 DataFrame
    val df = rdd.toDF("id","name","age")
    val ds = df.as[User]

    // 创建UDAF函数
    val udaf = new MyAgeAvgUDAFClass

    //将强类型的聚合函数转化为查询的列  -- 采用DSL语法进行访问
    ds.select(udaf.toColumn).show()

    //释放资源
    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)
  case class AvgBuffer(var totalAge: Int, var count: Int)


  /**
   * 功能描述 自定义聚合函数 - 强类型
   *  IN   输入数据类型 User
   *  BUF  缓冲区数据类型 AvgBuffer
   *  OUT  输出数据类型 Int
   * @return
   */
  class MyAgeAvgUDAFClass extends Aggregator[User,AvgBuffer,Int] {

    /**
     * 功能描述 缓冲区的初始值
     *
     * @param
     * @return
     */
    override def zero: AvgBuffer = {
      AvgBuffer(0,0)
    }

    /**
     * 功能描述 聚合函数
     *
     * @param
     * @return
     */
    override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
      buffer.totalAge = buffer.totalAge +  user.age
      buffer.count = buffer.count + 1
      buffer
    }

    /**
     * 功能描述
     *
     * @param
     * @return
     */
    override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
      buffer1.totalAge = buffer1.totalAge +  buffer2.totalAge
      buffer1.count = buffer1.count + buffer2.count
      buffer1
    }

    /**
     * 功能描述 计算函数的结果
     *
     * @param
     * @return
     */
    override def finish(result: AvgBuffer): Int = {
      result.totalAge / result.count
    }

    /**
     * 功能描述
     *
     * @param
     * @return
     */
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    /**
     * 功能描述
     *
     * @param
     * @return
     */
    override def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

}
