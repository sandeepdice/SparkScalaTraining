package com.sandeep.spark

import org.apache.spark.SparkContext
import scala.math.max

object MaxTemp extends App{
  val sc = new SparkContext("local[*]", "MaxTemp")
  
  /**
   * 	ITE00100554,18000101,TMAX,-75,,,E,
      ITE00100554,18000101,TMIN,-148,,,E,
      GM000010962,18000101,PRCP,0,,,E,
      EZE00100082,18000101,TMAX,-86,,,E,
      EZE00100082,18000101,TMIN,-135,,,E,
      ITE00100554,18000102,TMAX,-60,,I,E,
      ITE00100554,18000102,TMIN,-125,,,E,
      GM000010962,18000102,PRCP,0,,,E,
   * 
   */
  val rdd = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/1800.csv")
  
  // filter by given city name
  
  /**
   * ITE00100554,18000101,TMIN,-148,,,E,
   * EZE00100082,18000101,TMIN,-135,,,E,
   * ITE00100554,18000102,TMIN,-125,,,E,
   * 
   * (ITE00100554, -148), (EZE00100082, -135), (ITE00100554, -125) 
   */
  val allMinTemps = rdd.filter(line => line.contains("TMAX")).map(line => (line.toString().split(",")(0), line.toString().split(",")(3).toInt))
  
  // reducebykey:
  val minTemp = allMinTemps.reduceByKey((x, y) => max(x,y))
  
  minTemp.foreach(println)
  
}