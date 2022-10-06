package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkUDAFTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("用户自定义聚合函数")

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
    df.createTempView("user")

    // 创建UDAF函数
    val udaf = new MyAgeAvgUDAF

    // 注册UDAF
    spark.udf.register("myAvg",udaf)

    // 在SQL中使用UDAF
    spark.sql("select id,age,name from user").show()

    println("********************* UDAF *********************")

    spark.sql("select myAvg(age) from user").show()

    //释放资源
    spark.stop()
  }

  //自定义聚合函数
  class MyAgeAvgUDAF extends UserDefinedAggregateFunction {
    /**
     * 功能描述  输入数据的结构信息: 年龄
     *
     * @param
     * @return
     */
    override def inputSchema: StructType = {
      StructType(Array(StructField("age",IntegerType)))
    }

    /**
     * 功能描述  缓冲区的数据结构信息: 年龄总和,人数
     *
     * @param
     * @return
     */
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalAge",IntegerType),
        StructField("count",IntegerType),

      ))
    }

    /**
     * 功能描述  聚合函数返回的结果类型
     *
     * @param
     * @return
     */
    override def dataType: DataType = IntegerType

    /**
     * 功能描述 函数的稳定性(幂等性)
     *
     * @param
     * @return
     */
    override def deterministic: Boolean = true

    /**
     * 功能描述 函数缓冲区的初始化
     *
     * @param
     * @return
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
      buffer(1) = 0
    }

    /**
     * 功能描述 更新缓冲区
     *
     * @param
     * @return
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getInt(0)  + input.getInt(0)
      buffer(1) = buffer.getInt(1)  + 1
    }

    /**
     * 功能描述 合并缓冲区
     *
     * @param
     * @return
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getInt(0)  + buffer2.getInt(0)
      buffer1(1) = buffer1.getInt(1)  + buffer2.getInt(1)
    }

    /**
     * 功能描述 函数的计算 (totalAge / count)
     *
     * @param
     * @return
     */
    override def evaluate(buffer: Row): Any = {
      buffer.getInt(0) / buffer.getInt(1)
    }
  }
}
