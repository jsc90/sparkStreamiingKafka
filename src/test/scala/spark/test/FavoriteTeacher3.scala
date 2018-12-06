package spark.test

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by jiasichao on 2018/4/24.
  */
object FavoriteTeacher3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavoriteTeacher").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val subjectAndTeacher = lines.map(line => {
      val url = new URL(line)
      val host = url.getHost
      val subject = host.substring(0,host.indexOf("."))
      val teacher = url.getPath.substring(1)

      (subject , teacher)

    })
    //定义分区器
    val subjectPartitioner = new SubjectPartitioner1

    //先聚合 。 效率高
    val reducedAndPartitioned: RDD[((String, String), Int)] = subjectAndTeacher.map((_,1)).reduceByKey(subjectPartitioner,_+_)



    val result = reducedAndPartitioned.mapPartitions(_.toList.sortBy(_._2).reverse.take(2).iterator)

    println(result.collect())
//    效率低，因为有两次shuffle
//    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.map((_,1)).reduceByKey(_+_)
//    val partitioned = reduced.partitionBy(subjectPartitioner)


    sc.stop()
  }
}

class SubjectPartitioner1 extends Partitioner {
  //定义分区规则
  //读取学科信息
  val rules = Map("bigdata" ->1,"java"->2,"php"->3)

  //有几个分区
  override def numPartitions: Int = rules.size+1

  //根据传入的key 返回具体的分区。
  override def getPartition(key: Any): Int = {
    val tg = key.asInstanceOf[Tuple2[String,String]]
    rules.getOrElse(tg._1,0 )
  }
}
