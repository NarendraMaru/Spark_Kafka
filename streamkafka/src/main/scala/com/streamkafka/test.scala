package com.streamkafka


import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql.catalyst.expressions.aggregate.Collect
import org.apache.spark.sql.SQLContext


object test {
     def main (args: Array[String])
  {  
      val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf,Seconds(3))
      // nc -Lp 9999  
      val lines = ssc.socketTextStream("localhost" , 9999)
      val word = lines.flatMap(_.split(" "))
      //word.print()
      val pairs = word.map(word=>(word,1))
      //pairs.print()
      val wordCount = pairs.reduceByKey(_+_)
      wordCount.print()
      //wordCount.foreachRDD( rdd => { for(item <- rdd.collect().toArray) { println(item);}})
      
      
      ssc.start()
      ssc.awaitTermination()
           
  }

}