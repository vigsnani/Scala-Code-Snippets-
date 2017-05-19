package BigData.Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HWB3 {
  def main(args: Array[String])={
  val conf = new SparkConf();
  val sc = new SparkContext(conf);
  var input = sc.textFile(args(0))
  var output = input.map(line=>line.split(","))
  .map(line=>(line(1),line(2).toInt))
  .reduceByKey((a,b)=>(a+b))
  output.sortBy(_._1).sortByKey(true,1).saveAsTextFile(args(1))
  } 
}