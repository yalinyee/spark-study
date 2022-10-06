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
object SparkReadJdbcTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("从MySQL读取数据")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //从表MyUser读取数据
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "MyUser")
      .load()

    df.show()

    //将数据写入到新表MyUser1
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "MyUser1")
      .mode(SaveMode.Append)
      .save()

    //释放资源
    spark.stop()
  }

}
