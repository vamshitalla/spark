/*
 Author: Vamshi Talla
 Date  : 12/4/2017
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

object PriceOptimization {

  def main(args: Array[String]): Unit = {

    val path = "/Users/vamshitalla/Downloads/Python/products1.csv"

    val conf = new SparkConf().setAppName("Price Point Optimization").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val products = sc.textFile(path).map(line => line.split(",")).map(p => Products(p(0), p(1).trim.toFloat, p(2).trim.toFloat, p(3).trim.toFloat)).toDF()

    products.registerTempTable("products")

    products.show()

    val priceDecimal = sqlContext.sql("select Product, Minimum, round(round(Minimum, 1)+.03,2) as pricepoint3, round(round(Minimum, 1)+0.05,2) as pricepoint5, round(round(Minimum, 1)+0.09,2) as pricepoint9 from products")

    priceDecimal.show()

  }


}
