package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkReadCvsTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("从CSV文件读取数据")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //Spark SQL可以配置CSV文件的列表信息，读取CSV文件,CSV文件的第一行设置为数据列
    val df = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/user.csv")

    df.show()
    //释放资源
    spark.stop()
  }

}
