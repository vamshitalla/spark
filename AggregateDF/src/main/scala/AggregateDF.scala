/*
 Author: Vamshi Talla
 Date  : 12/07/2017
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext

object AggregateDF {

  def main(args: Array[String]): Unit = {

    val path="/Users/vamshitalla/IdeaProjects/spark/AggregateDF/data.txt"

    val conf = new SparkConf().setAppName("Aggregate DataFrame").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile(path)

    val header = data.first()

    val df = data.filter(row => row != header).map(line => line.split(",")).map(s => Sales(s(0), s(1), s(2).trim.toInt, s(3).trim.toInt)).toDF

    val newDF = df
      .withColumn("extended_price", df("quantity") * df("price"))
      .drop("item")
      .drop("price")
      .groupBy("name").sum("quantity", "extended_price")

    newDF.show()
  }

}
