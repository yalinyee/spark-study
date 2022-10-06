package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkLoadAndSaveTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("通用的读取和保存")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    import spark.implicits._

    //通用读取的文件格式默认为Parquet列式存储格式
    // val df: DataFrame = spark.read.load("input/users.parquet")

    //如果要改变读取文件的格式，需要特需操作
    //spark读取JSON文件时,要求每一行满足JSON格式(如果行不满足JSON格式,那么解析结果就不正确)
    val df: DataFrame = spark.read.format("json").load("input/user.json")

    // 简化版本的读取JSON文件
    // spark.read.json("input/user.json")

    //显示结果
    df.show()

    //通用的保存: SparkSql 通用文件保存格式为parquet格式
    // df.write.save("output")

    //保存的文件是指定的格式: 例如JSON
    // df.write.format("JSON").save("output")

    //保存模式：文件已经存在不会报错
    df.write.mode("overwrite").format("JSON").save("output")



    //释放资源
    spark.stop()
  }

}
