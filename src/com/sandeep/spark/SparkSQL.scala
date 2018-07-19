package com.sandeep.spark
import org.apache.spark.sql._

object SparkSQL extends App {

  case class Person(id: Int, name: String, age: Int, friendCount: Int)

  def mapper(line: String): Person = {
    Person(line.split(',')(0).toInt, line.split(',')(1), line.split(',')(2).toInt, line.split(',')(3).toInt)
  }
  val spark = SparkSession.builder.master("local").getOrCreate()

  val rdd = spark.sparkContext.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/fakefriends.csv")

  val persons = rdd.map(mapper)
  
  import this.spark.implicits._
  
  val schemaPeople = persons.toDS
  
  val tab = schemaPeople.registerTempTable("persons")
  
  val filtered = spark.sql("select * from persons where age > 13 and age < 19").collect()
  
  filtered.foreach(println)

}