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
    
    // reads the data into a fixed unbounded input table schema 
    // schema contains key, value, topic, partition, offset, timestamp, timestampType
    val df = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers","localhost:9092")
              .option("subscribe","sstpk")
              .load()
              
    // we know that data from kafka comes out in byte array serialized format
    // By casting it as a string, we can read it
    val df1 = df.withColumn("value", expr("cast(value as string)"))

    //processing - adding a date stamp column to the data
    val finaldf = df.withColumn("tdate", current_date)

    df1.writeStream
          .format("console") //use console if you are writing to console
          .option("checkpointLocation","file:///F:/checkpoint")
          .start() //use start if you want to write to a location
          .awaitTermination()

  }
  }