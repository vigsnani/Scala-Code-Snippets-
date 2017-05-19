package BigData.Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HWB2 {
  def main(args: Array[String])={
val conf = new SparkConf();
val sc = new SparkContext(conf);
var businessFile = sc.textFile(args(0))
var reviewFile = sc.textFile(args(1))
var review = reviewFile.map( line => (line.split("::")))
.map(line=>(line(2),(line(3).toDouble,1L)))
.reduceByKey((z,y)=>(z._1+y._1,z._2+y._2))
.map((a)=>(a._1,(a._2._1/a._2._2)))
.sortBy(-_._2)
.top(10)

var rev = sc.parallelize(review)

var bus = businessFile.map( line => (line.split("::")))
.map(line=>(line(0),(line(1),line(2))))

bus.join(rev).distinct().repartition(1).saveAsTextFile(args(2))

  }
}