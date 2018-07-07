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
  
  // extract age and friend counts. This generates (33, 385) (33,2)
  val ageFriendsCounts = rawFile.map(line => (line.toString().split(",")(2).toInt,  line.toString().split(",")(3).toInt))
  
  // map values
  /**
   * 	(26,(293,1))
			(24,(150,1))
			(54,(397,1))
			(54,(200,1))
   * 
   */
  val int1 = ageFriendsCounts.mapValues(x => (x,1))
  
  
  /**
   * groups two (54,(397,1)), (54,(200,1)) as: (54, (597,2))
   */
  val int2 = int1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
  
  // now calculate average using map
  
  val finalVal = int2.mapValues(x => x._1 / x._2)
  
  finalVal.foreach(println)
}