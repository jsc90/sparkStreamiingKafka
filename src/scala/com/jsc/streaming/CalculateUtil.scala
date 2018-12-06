package com.jsc.streaming

import com.jsc.core.MyUtils
import JedisConnectionPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by jiasichao on 2018/7/18.
  */
object CalculateUtil {


  def calculateIncome(fields: RDD[Array[String]]) = {
    /*将数据计算好，直接放到redis即可*/
    val priceRDD: RDD[Double] = fields.map(arr => {
      val cost = arr(4).toDouble
      cost
    })
    /*reduce是一个action，会把结果返回到Driver端
    * 将当前批次的总金额返回了*/
    val sum: Double = priceRDD.reduce(_+_)
    //获取一个jedis连接
    val conn = JedisConnectionPool.getConnection()
    //将历史值和当前的值进行累加
    //conn.set(Constant.TOTAL_INCOME,sum.toString)
    conn.incrByFloat(Constant.TOTAL_INCOME,sum);
    //释放连接
    conn.close()
  }

  def calculateItem(fields: RDD[Array[String]]) = {
    /*将数据计算好，直接放到redis即可*/
    val itemPrice: RDD[(String, Double)] = fields.map(arr => {
      //分类
      val item = arr(2)
      //金额
      val cost = arr(4).toDouble

      (item, cost)
    })
    //安商品分类进行聚合
     val itemTotal: RDD[(String, Double)] = itemPrice.reduceByKey(_+_)
    //现在这种方式，jedis的连接是在哪一端创建的（driver端）
    //在Driver端拿Jedis链接不好
   // val connection = JedisConnectionPool.getConnection()
    //将当前批次的数据累加到redis中
    //foreachPartition是一个action(本质上看有没有提交runJob方法)
    itemTotal.foreachPartition(part =>{
      //在executor中获取一个jedis链接
      val connection = JedisConnectionPool.getConnection()

      part.foreach(t =>{
        //这个链接其实是在Executor中获取的
        //JedisConnectionPool在一个Executor进程中有几个实例（单例）
        connection.incrByFloat(t._1,t._2)
      })

      //将分区内task更新完 再关闭

      connection.close()
    })

  }

  def calculateZone(fields: RDD[Array[String]],broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {
    val provinceAndPrice: RDD[(String, Double)] = fields.map(f => {
      val ip = f(1)
      val price = f(4).toDouble
      val ipNum = MyUtils.ip2Long(ip)
      //在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = MyUtils.binarySearch(allRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      //省份，订单金额
      (province, price)
    })
    //按省份进行聚合
    val reduce: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)
    reduce.foreachPartition(part =>{
      //获取一个jedis链接
      val connection = JedisConnectionPool.getConnection()
      part.foreach(t =>{
        //这个链接其实是在Executor中获取的
        //JedisConnectionPool在一个Executor进程中有几个实例（单例）
        connection.incrByFloat(t._1,t._2)
      })

      //将分区内task更新完 再关闭

      connection.close()
    })
  }
}
