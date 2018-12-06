package spark.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
  * Created by jiasichao on 2018/4/19.
  */
object FavoriteTeacher {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ScalaWordCount")

    val sc = new SparkContext(sparkConf)

    val textfile = sc.textFile(args(0))

    val wc = textfile.map(line =>
      {
        val fields: mutable.ArrayOps[String] = line.split("/")
        val teacher: String = fields(fields.size).toString
        val sub: String = fields(2).split(".")(0).toString
        (sub,teacher)
      }).map((_,1)).reduceByKey(_+_).map(x=>{
      (x._1._1,(x._1._2,x._2))
    }).groupByKey()

    wc.mapValues(_.toList.sortBy(_._2).reverse.take(2))


    wc.saveAsTextFile(args(1))

    sc.stop();
  }

}
