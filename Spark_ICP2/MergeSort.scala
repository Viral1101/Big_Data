import org.apache.spark._

import scala.collection.mutable.ListBuffer

object MergeSort {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")
    val conf = new SparkConf().setAppName("mergeSort").setMaster("local[*]")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(38,27,43,3,9,82,10))

    val sorted = input.sortBy(x=>x)

    println(sorted.collect().mkString(","))

  }
/*
  def function mergeSplit(a:ListBuffer[Int], b:ListBuffer[Int]) : ListBuffer[Int] = {

  }*/

}
