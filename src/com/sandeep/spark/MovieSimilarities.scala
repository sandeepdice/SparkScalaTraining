package com.sandeep.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math.sqrt

object MovieSimilarities {

  def loadMovieNames(line: String) {
    (line.split('|')(0), line.split('|')(1))
  }

  def filterDuplicates(ratingsPair: (Int, ((Int, Double), (Int, Double)))): Boolean = {
    return (ratingsPair._2._1._1 < ratingsPair._2._2._1)
  }

  def mapRatings(line: String): (Int, (Int, Double)) = {
    (line.split('\t')(0).toInt, (line.split('\t')(1).toInt, line.split('\t')(2).toDouble))
  }

  def computeCosineSimilarity(ratingPairs: Iterable[(Double, Double)]): (Double, Double) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    val ratings = sc.textFile("/home/sandeep/work/SparkScalaCourse/ml-100k/u.data").map(mapRatings)
    val joinedRatings = ratings.join(ratings)

    // [(Int, ((Int, Double), (Int, Double)))]
    val filteredRatings = joinedRatings.filter(filterDuplicates)

    // ((movieA, movieB), (ratingA, ratingB))
    val flippedRatings = filteredRatings.map(ratingPair => ((ratingPair._2._1._1, ratingPair._2._2._1), (ratingPair._2._1._2, ratingPair._2._2._2)))

    // ((movieA, movieB), [(ratingA, ratingB), (ratingC, ratingD), (ratingE, ratingF)])
    val groupedRatings = flippedRatings.groupByKey()

    val similarRatings = groupedRatings.mapValues(computeCosineSimilarity)
    
    similarRatings.foreach(println)
  }
}