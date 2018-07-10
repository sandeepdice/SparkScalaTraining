package com.sandeep.spark
import org.apache.spark.SparkContext

object CustomerSpend {
  
  def main(args : Array[String]) = {
  val sc = new SparkContext("local[*]", "CustomerSpend")
  
  var lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/customer-orders.csv")
  
  var words = lines.map(line => (line.split(",")(0).toInt, line.split(",")(2).toFloat))
  
//  var lowercaseWords = words.map(word => word.toLowerCase()).filter(x => !list.contains(x))
  
  // we can do the countByValue but it returns a scala map instead of RDD.
  // we can use collection utils to work on the map but we want to do it a distributed / scalable fashio
  // so we're implementing the counts by ourselves so it gives us the RDD
//  var counts = lowercaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)
  
//   we want to see the most popular words, so we need to flip the rdd
//  var invertedCounts = counts.map(x => (x._2, x._1))
  val agg = words.reduceByKey((x,y) => x+y).sortByKey(true)
//  agg.foreach(myprint)
  val result = agg.collect()
  result.foreach(println)
  }
  
  def myprint(x: (Int, Float)) =  {
    println(x._1 + " " + x._2)
  }
  
}

