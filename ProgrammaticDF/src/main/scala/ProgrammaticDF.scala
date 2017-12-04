/*
 Author: Vamshi Talla
 Date  : 12/03/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object ProgrammaticDF {

  def main(args: Array[String]): Unit = {

    val path = "/usr/local/Cellar/apache-spark/examples/src/main/resources/people.txt"

    val conf = new SparkConf().setAppName("Programmatic DF").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val people = sc.textFile(path)

    val schemaString = "name age"

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)

    peopleDF.registerTempTable("people")

    val results = sqlContext.sql("select name from people")

    results.show()
  }
}
