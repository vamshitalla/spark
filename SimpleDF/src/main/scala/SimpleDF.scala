/*
 Author: Vamshi Talla
 Date  : 12/02/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SimpleDF {

  def main(args: Array[String]): Unit = {

    val path = "/usr/local/Cellar/apache-spark/examples/src/main/resources/people.json"

    val conf = new SparkConf().setAppName("Simple DataFrame").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json(path)

    df.show()
  }

}

