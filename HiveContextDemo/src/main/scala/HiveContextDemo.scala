/*
 Author: Vamshi Talla
 Date  : 12/03/2017
 */


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive.HiveContext

object HiveContextExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("HiveContext Demo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

    hiveCtx.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING)")

    hiveCtx.sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/apache-spark/2.2.0/libexec/examples/src/main/resources/kv1.txt' OVERWRITE INTO TABLE src")

    hiveCtx.sql("SELECT key, value FROM src").collect().foreach(println)


  }

}