package com.sandeep.spark

import org.apache.spark.SparkContext

object MostPopularMovie {
  def main(args : Array[String]) = {
     var sc = new SparkContext("local[*]", "MostPopularMovie")
     
     /**
      * userId,movieId,rating,timestamp
				1,2,3.5,1112486027
				1,29,3.5,1112484676
      * 
      */
     val lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/ml-20m/ratings.csv")
     val movies = lines.map(line => (line.toString().split(",")(1), 1))
     val ratingCounts = movies.reduceByKey((x,y) => x + y).map(x => (x._2, x._1)).sortByKey(false)
     
     println(ratingCounts.first())
  }
}