package com.sandeep.spark
import org.apache.spark.SparkContext

object WordCountBetter extends App{
  val sc = new SparkContext("local[*]", "MaxPrecipitation")
  
  var lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/book.txt")
  
  var words = lines.flatMap(line => line.split("\\W+"))
  
  var lowercaseWords = words.map(word => word.toLowerCase())
  
  var counts = lowercaseWords.countByValue()
  
  counts.foreach(println)
}