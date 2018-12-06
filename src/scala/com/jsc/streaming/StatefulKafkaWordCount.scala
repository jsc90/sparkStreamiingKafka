package com.jsc.streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiasichao on 2018/7/13.
  */
object StatefulKafkaWordCount {


  /*
  *  第一个参数：聚合的key,就是单词
  *  第二个参数：当前批次该单词在每一个分区出现的次数（不同分区的不同数量）
  *  第三个参数：初始值或累加的中间结果
  * */
  //(Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  val updateFunc = (iter: Iterator[(String,Seq[Int],Option[Int])])=> {
//    iter.map(t => (t._1,t._2.sum +t._3.getOrElse(0)))
    iter.map{ case (x,y,z) => (x,y.sum+z.getOrElse(0))}
  }

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //如果要使用更新历史数据，那么就要把中间结果保存起来
    ssc.checkpoint("./ck")

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
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果(print 是 action)
    reduced.print()

    //启动sparksteaming程序
    ssc.start()


    //等待优雅的退出
    ssc.awaitTermination()
  }
}
