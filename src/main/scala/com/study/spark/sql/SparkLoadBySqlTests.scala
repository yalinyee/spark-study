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
object SparkLoadBySqlTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("直接在SQL中读取文件")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.sql("select * from json.`input/user.json` ").show()

    //释放资源
    spark.stop()
  }

}
