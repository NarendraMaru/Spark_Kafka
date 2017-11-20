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

object file {
	
    def main (args: Array[String])
     { 
    	
     val sparkConf = new SparkConf().setAppName("Kafka").setMaster("local[2]")
     val sc = new SparkContext(sparkConf)
     val ssc = new StreamingContext(sc,Seconds(10))
     val lines = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", Map("test1" -> 10))
      
      
    lines.foreachRDD( line => {for(line <- line.collect().toArray){}}) 
    //lines.print()
    
    val nLine = lines.flatMap{case (x,y) => y.split("/n")} //case not understood
    //nLine.print()
         
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
       
    nLine.map(_.toString()).foreachRDD(rdd => {
    rdd.foreach(println)
    if (!rdd.isEmpty()) {
    rdd.toDF("value").coalesce(1).write.mode(SaveMode.Append).json("E:\\CSV Files\\Kafka\\output.json")
    }

})

      //nLine.count()
     ssc.start()
     ssc.awaitTermination()
     }
}