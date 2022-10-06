package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkInnerHiveTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("操作SparkSQL内置的Hive")

    //创建SparkSession  默认情况下SparkSQL支持本地Hive操作的,执行前需要启用Hive的支持
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // spark.sql("create table test1(id int)")
    // spark.sql("show tables").show

    spark.sql("load data local inpath 'input/ids.txt' into table test1")
    spark.sql("select * from test1").show

    //释放资源
    spark.stop()
  }

}
