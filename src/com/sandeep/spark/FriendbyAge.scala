package com.sandeep.spark

import org.apache.spark.SparkContext

object FriendbyAge extends App {
  
  val sc = new SparkContext("local[*]", "FriendsByAge")
  val rawFile = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/fakefriends.csv")
  /**
   * 	0,Will,33,385
   * 	3,Bruce,33,2
			1,Jean-Luc,26,2
			2,Hugh,55,221
   * 
   */
  
  // extract age and friend counts. This generates (33, 385) (33,2), (26,2), (55,221)
  val ageFriendsCounts = rawFile.map(line => (line.toString().split(",")(2).toInt,  line.toString().split(",")(3).toInt))
  
  // next line generates
  /**
   * 	(33,(385,1))
			(33,(2,1))
			(26,(2,1))
			(55,(221,1))
   * 
   */
  val int1 = ageFriendsCounts.mapValues(x => (x,1))
  
  
  /**
   * groups two (33,(385,1)), (33,(2,1)) as: (33, (387,2))
   */
  val int2 = int1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
  
  // now calculate average using map
  
  val finalVal = int2.mapValues(x => x._1 / x._2)
  
  finalVal.foreach(println)
  
  
  /**
   * Map methods always takes function that takes one parameter
   * Reduce methods always takes function that takes two parameters i.e., two values of the same key
   * 
   * We always have mapValues but never reduceValues 
   */
}