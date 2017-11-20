package com.streamkafka


import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.{SQLContext, SaveMode}
import scala.collection.immutable.Set
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row

object append {
  def main (args: Array[String])
     { 
     val sparkConf = new SparkConf().setAppName("Kafka").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
     // The master requires 2 cores to prevent from a starvation scenario.
     val ssc = new StreamingContext(sc,Seconds(5))
     //ssc.checkpoint("checkpoint")
    // val lines = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", Map("test1" -> 5))
      val lines = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", Map("test1" -> 10))
      
      
      // lines.print()
      
      
    lines.foreachRDD( rdd => {for(x <- rdd.collect().toArray){println(x);}})  
    lines.saveAsTextFiles("E:\\CSV Files\\Kafka\\")
    lines.print()
    
    //////////working code
    /*val nLine = lines.flatMap{case (x, y) => y.split("/n")}
   // nLine.print()
    */  
    /////////////////////////////// working code
    /*val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    nLine.map(_.toUpperCase()).foreachRDD(rdd => {
    rdd.foreach(println)
    if (!rdd.isEmpty()) {
        rdd.toDF("value").coalesce(1).write.mode(SaveMode.Append).json("E:\\CSV Files\\Kafka\\output.json")
        // rdd.saveAsTextFile("C:/data/spark/")
    }

})
*/    
    
    ////////////////////////////////////////////
    /*
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
        
    nLine.foreachRDD(rdd=>{
        import sqlContext.implicits._
        val maptorow = lines.foreachRDD(rdd=>{
        val newRDD = rdd.map(line => getMap(line))
          .map(p =>
          Row(p.get("column1"),
            p.get("column2"))    
        val myDataFrame = newRDD.toDF())})})
        
//process myDataFrame as a DF
    */  
    //////////////////////////////////////////
    
/*    lines.foreachRDD { (rdd: RDD[String], time: Time) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()
      wordsDataFrame.createOrReplaceTempView("lines")
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }
*/    
    
    //val newDF = spark.createDataFrame(lines,df.schema)
   
    
    /*lines.foreachRDD { rdd =>print()
  import org.apache.spark.sql.SQLContext
  val dataFrame = rdd.map {case (key, value) => Row(key, value)}.toDf()
}*/
    
    
    
    
  
    //val sc = new SparkContext()
    //val sqlContext = new  org.apache.spark.sql.SQLContext(sc)
     
    
     //val Drdd = new ListBuffer[RDD[String]] 
     //lines.foreachRDD(rdd => {Drdd += rdd })
     
     //val op = lines.saveAsTextFiles("kafka", "txt")
     //val op =  lines.foreachRDD(rdd => rdd.foreachPartition {partition => partition.foreach {file => runConfigParser(file)}})
     
     ssc.start()
     ssc.awaitTermination()
     }

}