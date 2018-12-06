package com.jsc.streaming

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by jiasichao on 2018/7/22.
  */
object IPUtils {
  def broadcastIpRules(sparkContext: SparkContext, path: String):Broadcast[Array[(Long, Long, String)]] = {
    //1.ip规则数据
    val ipRulesLine: RDD[String] = sparkContext.textFile(path)

    val rules: RDD[(Long, Long, String)] = ipRulesLine.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)

      (startNum, endNum, provice)
    })
    val ipRules = rules.collect()
    //广播ip规则
    sparkContext.broadcast(ipRules)

  }

}
