package com.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 功能描述
 *  在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 import spark.implicits._
 *
 * @param
 * @return
 */
object SparkUDFTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("用户自定义函数")

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

    //使用自定义函数,完成数据转换工作
    spark.udf.register("addName",(x: String) => "name:" + x)
    spark.udf.register("changeAge",(x: Int) => 18)


    spark.sql("select id,age,name from user").show()

    println("********************* UDF *********************")

    spark.sql("select id, changeAge(age), addName(name) as name from user").show()

    //释放资源
    spark.stop()
  }

}
