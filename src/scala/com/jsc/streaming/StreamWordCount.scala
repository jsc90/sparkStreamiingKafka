package com.jsc.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/7/12.
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //离线是sparkContext,实时计算是StreamingContext
    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    //第二个参数，是小批次产生的时间间隔
    val ssc = new StreamingContext(sc,Milliseconds(10000))
    //有了StreamingContext,就可以创建SparkStreaming的抽象了DStream
    //从socket读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("20.111.55.5",8888)
    //对DStream进行操作，你操作这个抽象，就像操作一个本地的集合

    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词跟一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果(print 是 action)
    reduced.print()

    //启动sparksteaming程序
    ssc.start()


    //等待优雅的退出
    ssc.awaitTermination()
  }
}
