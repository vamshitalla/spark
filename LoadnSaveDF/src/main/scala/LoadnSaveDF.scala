/*
 Author: Vamshi Talla
 Date  : 12/03/2017
 */


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext


object LoadnSaveDF {

  def main(args: Array[String]): Unit = {

    val path = "/usr/local/Cellar/apache-spark/examples/src/main/resources/users.parquet"

    val conf = new SparkConf().setAppName("Load n Save DF").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load(path)

    df.select("name", "favorite_color").write.mode("overwrite").save("namesAndFavColors.parquet")

    df.show()

  }


}
