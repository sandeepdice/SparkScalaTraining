package com.sandeep.spark

import org.apache.spark.SparkContext

object AvgFriendsByFirstName extends App{
  val sc = new SparkContext("local[*]", "AvgFriendsByFirstName")

  /**
   * 	0,Will,33,385
   * 	3,Bruce,33,2
			1,Jean-Luc,26,2
			2,Will,55,221
   * 
   */
  val rdd = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/fakefriends.csv")
  
  // (will, 385), (Bruce, 2), (Jean, 2), (Will, 221)
  val nameFriends = rdd.map(line => (line.toString().split(",")(1), line.toString().split(",")(3).toInt))
  
  // (will, (385,1)) (Bruce, (2,1)), (Jean, (2,1)), (Will, (221,1))
  val int1 = nameFriends.mapValues(x => (x, 1))
  
  // (will, (606, 2)) (Bruce, (2,1)), (Jean, (2,1))
  val int2 = int1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
      
  // (will, 303), (Bruce, 2) (Jean, 2)
  int2.mapValues(x => x._1 / x._2).foreach(println)
  
}