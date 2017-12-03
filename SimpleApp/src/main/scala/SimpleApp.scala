/*
 Author: Vamshi Talla
 Date  : 12/02/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {

  def main(args: Array[String]): Unit = {

    val logFile = "/Users/vamshitalla/Derby.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    println("Number of As: %s, Number of Bs: %s" .format(numAs,numBs))
  }

}
