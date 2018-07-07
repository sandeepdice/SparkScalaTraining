package com.sandeep.spark

import org.apache.spark.SparkContext

object MyRatingsCounter {
  def main(args : Array[String]) = {
     var sc = new SparkContext("local[*]", "MyFirstApp")
     
     val lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/ml-20m/ratings.csv")
     val ratings = lines.map(line => line.toString().split(",")(2))
     val ratingCounts = ratings.countByValue()
     ratingCounts.toSeq.sortBy(_._1).foreach(println)
  }
}