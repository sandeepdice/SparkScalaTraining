package com.sandeep.spark
import org.apache.spark.SparkContext

object WordCount extends App{
  val sc = new SparkContext("local[*]", "MaxPrecipitation")
  
  var lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/book.txt")
  
  var words = lines.flatMap(line => line.split(" "))
  
  var counts = words.countByValue()
  
  counts.foreach(println)
}