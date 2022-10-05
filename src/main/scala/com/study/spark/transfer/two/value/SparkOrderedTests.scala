package com.study.spark.transfer.two.value

import org.apache.spark.{SparkConf, SparkContext}

object SparkOrderedTests {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" Ordered  Test: 自定义排序规则!").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      (new User(2),1),
      (new User(1),2),
      (new User(3),3),

    ),2)

    val sortByKeydd  = rdd.sortByKey(true)

    println(sortByKeydd.collect().mkString(","))
    sc.stop()
  }

  class User(val id: Int) extends Ordered[User] with Serializable {
    override def compare(that: User): Int =
    {
      this.id - that.id
    }

    override def toString = s"User($id)"
  }
}
