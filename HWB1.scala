package BigData.Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HWB1 {
  def main(args: Array[String])={
  val conf = new SparkConf();
  val sc = new SparkContext(conf);
  var business = sc.textFile(args(0))
  var review = sc.textFile(args(1))
  

var businessData = business.map(line=>line.split("::"))
.filter(line=>line(1).contains("Stanford"))
.map(line=>(line(0),line(1)))

var reviewData = review.map(line=>line.split("::"))
.map(line=>(line(2),(line(1),line(3))))

var output = reviewData.join(businessData).distinct().map(line=>(line._2._1._1,line._2._1._2)).repartition(1).saveAsTextFile(args(2))

  }
  
}