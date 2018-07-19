package com.sandeep.spark
import org.apache.spark.sql._

object DataFrames extends App {

  case class Person(id: Int, name: String, age: Int, friendCount: Int)
  
  def mapper(line: String) : Person = {
    Person(line.split(',')(0).toInt, line.split(',')(1), line.split(',')(2).toInt, line.split(',')(3).toInt)
  }
  
  val spark = SparkSession.builder().appName("DataFrames").master("local").getOrCreate()

  // map the file and generate RDD[Person]
  val personRdd = spark.sparkContext.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/fakefriends.csv").map(mapper)
  
  import this.spark.implicits._
  // convert RDD to DF
  val persons = personRdd.toDS
  
  // run select commands on DS
  persons.select("name").show()
  
  persons.filter(persons("age") < 20 ).show()
}