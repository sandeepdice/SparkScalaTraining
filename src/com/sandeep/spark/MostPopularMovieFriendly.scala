package com.sandeep.spark

import org.apache.spark.SparkContext
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostPopularMovieFriendly {
  def main(args: Array[String]) = {

    def readMovies(): Map[Int, String] = {
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      var movieMap: Map[Int, String] = Map();
      val fileData = Source.fromFile("/home/sandeep/work/SparkScalaCourse/ml-20m/movies.csv").getLines();
      for (line <- fileData) {
        var splits = line.split(",")
        if (splits.length > 1) {
          movieMap += (splits(0).toInt -> splits(1))
        }
      }
      return movieMap;
    }

    var sc = new SparkContext("local[*]", "MostPopularMovieFriendly")

    /**
     * userId,movieId,rating,timestamp
     * 1,2,3.5,1112486027
     * 1,29,3.5,1112484676
     *
     */
    val lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/ml-20m/ratings.csv")
    val movies = lines.map(line => (line.toString().split(",")(1), 1))
    val ratingCounts = movies.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false)
    var brodcastData = sc.broadcast(readMovies)

    println(brodcastData.value(ratingCounts.first()._2.toInt))

  }
}