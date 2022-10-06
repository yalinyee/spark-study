package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 * @param
 * @return
 */
object SparkCreateDataFrameTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Create DataFrame")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //导入隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    import spark.implicits._

    //创建 DataFrame
    val df = spark.read.json("input/user.json")

    //将 DataFrame 转化为临时视图
    df.createTempView("user")

    //使用 SQL 查询数据
    spark.sql("select * from user").show()

    //spark的隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    df.select('name).show()
    df.select('name,'age).show()

    //释放资源
    spark.stop()
  }
}
