package mergeFiles

import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.Text

object MergeFiles extends App{
  val sc = new SparkContext("local[*]", "AvgFriendsByFirstName")

//    val input = sc.newAPIHadoopFile("/home/sandeep/Downloads/twitter.snappy.avro", classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text])
//    println(input.collect().toList)

  
}