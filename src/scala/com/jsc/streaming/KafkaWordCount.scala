package com.jsc.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiasichao on 2018/7/13.
  */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val zkQuorum = "myspark01:2181,myspark02:2181,myspark03:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("xiaoniu2" -> 1)
    //创建DStream,需要kafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)
    //Kafka的ReceiverInputDStream[(String,String)]里面装的是一个元祖（key是写入的key,value是实际写入的内容）
    val lines = data.map(_._2)

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
