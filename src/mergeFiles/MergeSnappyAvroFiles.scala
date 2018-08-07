package mergeFiles

import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import org.apache.spark._
//import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat
import org.apache.hadoop.io.{LongWritable, MapWritable, Text, BooleanWritable}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import java.util.HashMap
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.Text

object MergeSnappyAvroFiles extends App {
    val sc = new SparkContext("local[*]", "LoadJsonWithElephantBird")
    val input = sc.newAPIHadoopFile("/home/sandeep/Downloads/twitter.snappy.avro", classOf[SequenceFileInputFormat], classOf[Text], classOf[Text])
    println(input.collect().toList)
}


// https://github.com/databricks/learning-spark/blob/6b34161e2c1351500784a0d500c664c90846cacf/src/main/scala/com/oreilly/learningsparkexamples/scala/LoadJsonWithElephantBird.scala
// https://github.com/aws-samples/emr-bootstrap-actions/blob/master/spark/examples/spark-java-TeraInputFormat-count.md
// https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/input/package-summary.html
// https://stackoverflow.com/questions/11632067/reading-a-simple-avro-file-from-hdfs
// https://stackoverflow.com/questions/40091673/error-while-using-newapihadoopfile-api
// https://jobs.zalando.com/tech/blog/solving-many-small-files-avro/?gh_src=4n3gxh1