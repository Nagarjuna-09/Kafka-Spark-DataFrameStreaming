package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object obj {

  def main(args: Array[String]): Unit = {

     //required for writing the file from eclipse
    System.setProperty("hadoop.home.dir","D:\\hadoop")

    val conf = new SparkConf().setAppName("first")
                              .setMaster("local[*]")
                              .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate( )
    import spark.implicits._
    

  }
  }