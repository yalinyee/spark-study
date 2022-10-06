package com.study.spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}

object SparkSerializableTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Serializable Test: !").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))

//    rdd.foreach(num =>
//    {
//      val user = new User()
//      println("age: " + (user.id + num))
//    })

    //Exception : Task not serializable
    //如果算子中使用了外的对象,那么在执行时需要序列化
    val user = new User()
    rdd.foreach(num =>
    {
      println("age: " + (user.age + num))
    })


    sc.stop()
  }

//  class User extends Serializable {
//    val age :Int = 20
//  }

  //样例类自动混入了可序列化特质
  case class  User(age: Int = 20)
}
