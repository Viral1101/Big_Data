/**
  * Illustrates flatMap + countByValue for wordcount.
  */


import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object MatrixMultiply {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")
    val conf = new SparkConf().setAppName("matrixMultiply").setMaster("local[*]")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Get the input files defining the two matrices
    val input1 = sc.textFile("matrix1.txt")
    val input2 = sc.textFile("matrix2.txt")

    // Split the lines into each separate number to get the total amount of numbers
    val test = input1.flatMap(line => line.split(" "))
    val test2 = input2.flatMap(line => line.split(" "))

    // Organize the matrices into lists mapped to the corresponding rows
    val lines1 = input1.zipWithIndex().map{case (line,i) => (i,line.split(" ").toList)}
    val lines2_1 = input2.zipWithIndex().map{case (line,i) => (i,line.split(" ").toList)}

    // Calculate the rows and columns in each matrix
    val rows1 = lines1.count().toInt
    val cols1 = test.count().toInt/rows1

    val rows2 = lines2_1.count().toInt
    val cols2 = test2.count().toInt/rows2

    // Organize the matrix into ((row,col),number) at first
    val matrix1 = input1.zipWithIndex().flatMap{case (line,i) => line.split(" ").toList}.zipWithIndex().map{case (x,i)=>
      var listRow = ListBuffer[(Int,Int)]()
      for (i<-0 to rows1-1){
        for (j<-0 to cols1-1){
          listRow.append((i,j))
        }
      }

      (listRow(i.toInt),x)}
      //Reorganize the matrix into (row,(col,number)) to allow for grouping by the row key
      .map(x => (x._1._1,(x._1._2,x._2.toInt)))
      .groupByKey()

    // Organize the matrix into ((col,row),number) at first
    val matrix2 = input2.zipWithIndex().flatMap{case (line,i) => line.split(" ").toList}.zipWithIndex().map{case (x,i)=>
      var listRow = ListBuffer[(Int,Int)]()
      for (i<-0 to rows2-1){
        for (j<-0 to cols2-1){
          listRow.append((j,i))
        }
      }

      (listRow(i.toInt),x.toInt)}

      //Reorganize the matrix into (col,(row,number)) to allow for grouping by the col key
      .map(x => (x._1._1,(x._1._2,x._2.toInt)))
      .groupByKey()

    // Process the matrix multiplication
    val matrix3 = matrix2
      //Match each row in matrix 1 with each column in matrix 2
      // using a cartesion join
      .cartesian(matrix1)
      .map{case (x,y)=>
          //Store the matrix multiplication output to a mutable variable
          var out=0

          //Create list buffers to store each number
          val l1 = ListBuffer[Int]()
            x._2.foreach(f=> l1.append(f._2.toInt))
          val l2 = ListBuffer[Int]()
            y._2.foreach(f=>l2.append(f._2.toInt))

          //Iterate over each number in the listbuffers and multiply them
          // then add up the result for matrix multiplcation
          for(i<-0 to l2.size - 1){
            out += l1(i) * l2(i)
          }

        //Output the results as
        // ((row from matrix 1, col from matrix 2),result)
        ((y._1,x._1),out)
      }
      //Reorganize the data as (row,(col,result)) to group the output by the row key
      .map(x=> (x._1._1,(x._1._2,x._2)))
      .groupByKey()
      //Reorganize the output as a matrix similar to the input
      .map{case (x) =>

        var list = ListBuffer[Int]()

        for(i<-0 to x._2.size-1){
          list.append(-1)
        }

        val nums = x._2.toArray

        for(j<-0 to x._2.size-1){
          list.update(nums(j)._1,nums(j)._2)
        }
          list.mkString(""," ","\n")
      }

    //Display the output
    println(matrix3.collect().mkString(""))

  }
}
