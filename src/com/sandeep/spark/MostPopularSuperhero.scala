package com.sandeep.spark

object MostPopularSuperhero extends App {
  
  def parseNames(line: String) = {
    var splits = line.split('\"')
    if(splits.length > 1) {
      Some(splits(0).trim().toInt, splits(1).trim())
    } else {
      None
    }
  }
  
  def parseConnections(line: String) = {
    var splits = line.split("\\s+")
    (splits(0).trim().toInt, splits.length-1)
  }  
  
  import org.apache.spark.SparkContext
  var sc = new SparkContext("local[*]", "MostPopularSuperhero")

  val names = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/Marvel-names.txt")
  val pairings = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/Marvel-graph.txt")
  
  val namesRdd = names.flatMap(parseNames)
//  println(namesRdd.max())
  val pairingsRdd = pairings.map(parseConnections).reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey(false)
  println(pairingsRdd.first())
  val topHero = pairingsRdd.first()
  println(s" most popular: ${namesRdd.lookup(topHero._2)(0)} with mentions: ${topHero._1}")

}