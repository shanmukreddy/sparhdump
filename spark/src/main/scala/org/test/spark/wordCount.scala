package org.test.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._

object wordCount {
  def main(args: Array[String])={
   
    val conf = new SparkConf().setAppName("wordCount").setMaster("local");
    val sc = new SparkContext(conf);
    val test = sc.textFile("food.txt");
    
    test.flatMap { line => line.split(" ") }.map { word => (word,1) }.reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
    
  }
}