package com.streamkafka

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream._


object kafka {

       def main (args: Array[String])
     { 

     val sparkConf = new SparkConf().setAppName("Kafka").setMaster("local[2]")
     val ssc = new StreamingContext(sparkConf,Seconds(3))
     //ssc.checkpoint("checkpoint")
    // val lines = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", Map("test1" -> 5))
     // val lines = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", Map("nikhil" -> 1))
      val lines = KafkaUtils.createStream(ssc, "192.168.6.133:2181", "test-consumer-group", Map("test.test.naren" -> 1))
     lines.print() 
     
     ssc.start()
     ssc.awaitTermination()
     }

}