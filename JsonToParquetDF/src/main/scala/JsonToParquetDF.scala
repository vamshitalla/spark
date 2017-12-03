/*
 Author: Vamshi Talla
 Date  : 12/03/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

object JsonToParquetDF {

  def main(args: Array[String]): Unit = {

    val path = "/usr/local/Cellar/apache-spark/2.2.0/libexec/examples/src/main/resources/people.json"

    val conf = new SparkConf().setAppName("Programmatic DF").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load(path)

    people.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    //Query the file directly with SQL instead of loading it into a DF using read API and query it
    val df = sqlContext.sql("select * from parquet. `/Users/vamshitalla/IdeaProjects/spark/JsonToParquetDF/namesAndAges.parquet`")
  }
}
