package spark.test

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * create SparkContextApp
  */
object SparkContextApp {

  def main(args: Array[String]): Unit = {
    var conn: Connection = null
    var pstm: PreparedStatement = null
    val sql1 = "select t.table_name from information_schema.TABLES t where t.TABLE_SCHEMA ='insight_xscreen' and t.TABLE_NAME ='ssp_media_dmp_201811';"
    conn =  DriverManager.getConnection("jdbc:mysql://117.78.61.202:3311/insight_xscreen?useUnicode=true&characterEncoding=UTF-8", "miaozhen", "zY13hy@#!lN5v9&J")
    pstm = conn.prepareStatement(sql1)
    val res = pstm.executeQuery()
    if (res.next()) {
      println("fafafafafaf")
    }
    println("fafafafafaf2")

  }



}
