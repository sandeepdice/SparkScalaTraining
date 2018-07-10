package com.sandeep.spark
import org.apache.spark.SparkContext

object HighestSpentCustomer {

  def main(args: Array[String]) = {
    val sc = new SparkContext("local[*]", "HighestSpentCustomer")

    var lines = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/customer-orders.csv")

    var words = lines.map(line => (line.split(",")(0).toInt, line.split(",")(2).toFloat))

    var agg = words.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false)
    println(agg.first()._2);
  }

  def myprint(x: (Float, Int)) = {
    println(x._1 + " " + x._2)
  }

}