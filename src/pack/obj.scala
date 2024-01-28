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
    
    //read stream from kafka topic "sstpk"
    val df = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers","localhost:9092")
              .option("subscribe","sstpk")
              .load()
              
              
    //processing 
     val df1 = df.withColumn("value",expr("concat(value, ' sai')"))

    //writing the data to kafka ssouttpk topic
    df1.writeStream
          .format("kafka") //use console if you are writing to console
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("topic","ssouttpk")
          .option("checkpointLocation","file:///F:/checkpoint")
          .start() //use start if you want to write to a location
          .awaitTermination()
    
    //writing the result to cassandra table
//    df1.writeStream
//          .foreachBatch{(df:DataFrame, id:Long)=>
//            df.write.format("org.apache.spark.cassandra")
//                    .option("spark.cassandra.connection.host","localhost")
//                    .option("spark.cassandra.connection.port","9042")
//                    .option("keyspace","zeyok")
//                    .option("table","zeyotk")
//                    .save()
//            }
//          .option("checkpointLocation","file:///F:/checkpoint")
//          .start() //use start if you want to write to a location
//          .awaitTermination()

  }
  }

