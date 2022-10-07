package com.study.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * 功能描述 自定义采集器
 *
 * @param
 * @return
 */
object SparkCustomReceiverTests {
  def main(args: Array[String]): Unit = {
    // 初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("自定义数据采集器")

    // 初始化SparkStreamingContext,3秒钟采集一次
    val ssc = new StreamingContext(conf,Seconds(3))

    val ds = ssc.receiverStream(
      new MySqlReceiver("jdbc:mysql://localhost:3306/spark_sql",
                            "root",
                        "123456",
                         "MyUser")
    )

    ds.print()

    // 启动采集器
    ssc.start()

    // 等待采集器结束
    ssc.awaitTermination()
  }

  /**
   * 功能描述 自定义数据采集器(从MYSQ中获取数据)
   *
   * @return
   */
  class MySqlReceiver(srcConnStr: String, srcUser: String, srcPassword: String, tableName: String) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var connection: Connection = _

    /**
     * 功能描述
     *
     * @param
     * @return
     */
    override def onStart(): Unit = {
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection(srcConnStr,srcUser, srcPassword)
      // Start the thread that receives data over a connection
      new Thread("MySQL Receiver") {
        setDaemon(true)
        override def run() { receive() }
      }.start()
    }

    /**
     * 功能描述
     *
     * @param
     * @return
     */
    override def onStop(): Unit = {
      connection.close()
      connection = null
    }

    /**
     * 功能描述 接受数并将获取的数据保存到Spark框架内部进行封装
     *
     * @param
     * @return
     */
    def receive(): Unit = {

      val st = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val strSQL = "SELECT  name  FROM " + s"$tableName"  ;
      val rs = st.executeQuery(strSQL)
      while (rs.next) {
        store(rs.getString("name"))
      }
    }
  }
}
