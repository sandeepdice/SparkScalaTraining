package com.sandeep.spark

object Top10Superheros extends App {
  
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
  var sc = new SparkContext("local[*]", "Top10Superheros")

  val names = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/Marvel-names.txt")
  val pairings = sc.textFile("/home/sandeep/work/SparkScalaCourse/SparkScala/Marvel-graph.txt")
  
  val namesRdd = names.flatMap(parseNames)
//  println(namesRdd.max())
  val pairingsRdd = pairings.map(parseConnections).reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey(false)
  var top10 = pairingsRdd.take(10)
  for((x,y) <- top10) {
   println(s" most popular: ${namesRdd.lookup(y)(0)} with mentions: ${x}") 
  }
 
  val leastPopularPairingsRdd = pairings.map(parseConnections).reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey()
  var least10 = leastPopularPairingsRdd.take(10)
  for((x,y) <- least10) {
   println(s" least popular: ${namesRdd.lookup(y)(0)} with mentions: ${x}") 
  }

}