package com.sandeep.spark
import org.apache.spark.SparkContext

object WordCountSortStopList extends App{
  val sc = new SparkContext("local[*]", "WordCountSortStopList")
  
  var lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/book.txt")
  
  var words = lines.flatMap(line => line.split("\\W+"))
  
  var list = List("a", "the", "an", "is", "at", "on", "which", "of", "and", "it", "for", "in")
  
  var lowercaseWords = words.map(word => word.toLowerCase()).filter(x => !list.contains(x))
  
  // we can do the countByValue but it returns a scala map instead of RDD.
  // we can use collection utils to work on the map but we want to do it a distributed / scalable fashio
  // so we're implementing the counts by ourselves so it gives us the RDD
  var counts = lowercaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)
  
  // we want to see the most popular words, so we need to flip the rdd
  var invertedCounts = counts.map(x => (x._2, x._1))
  
  invertedCounts.sortByKey(false).foreach(println)
  
}