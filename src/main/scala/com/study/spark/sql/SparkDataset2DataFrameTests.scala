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
object SparkDataset2DataFrameTests {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("将 Dataset 转换为 DataFrame")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    import spark.implicits._


    val rdd = spark.sparkContext.makeRDD(List(
      (1,"张三",20),
      (2,"李四",30),
      (3,"王五",40)
    ))

    val df = rdd.toDF("id","name","age")

    //将 DataFrame 转换为 Dataset
    val ds: Dataset[User] = df.as[User]

    println("***************** DataFrame 转换为 Dataset************************")
    //spark的隐式转换,这里的spark其实就是环境对象的名称,要求这个变量spark必须使用val声明
    //使用 SQL 查询数据
    ds.select('name,'age).show()

    println("***************** Dataset 转换为 DataFrame************************")
    //将 Dataset 转换为 DataFrame
    val dsToDf = ds.toDF()

    //使用 SQL 查询数据
    dsToDf.select('name,'age).show()
    
    //释放资源
    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)
}
