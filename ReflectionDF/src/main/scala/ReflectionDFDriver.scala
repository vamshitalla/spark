/*
 Author: Vamshi Talla
 Date  : 12/0/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object ReflectionDFDriver {

  def main(args: Array[String]): Unit = {

    val path = "/usr/local/Cellar/apache-spark/examples/src/main/resources/people.txt"

    val conf = new SparkConf().setAppName("Reflection DF").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val people = sc.textFile(path).map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF

    people.registerTempTable("people")

    val teenagers = sqlContext.sql("select name, age from people where age >= 13 and age <= 19").show()
  }

}
