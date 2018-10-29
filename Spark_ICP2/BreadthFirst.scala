import org.apache.spark._

import scala.collection.mutable.ListBuffer

object BreadthFirst {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")
    val conf = new SparkConf().setAppName("breadthFirst").setMaster("local[*]")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    //Tree structure by text input

    /*
    val input1 = sc.textFile("graph.txt").flatMap(lines => lines.split(" "))

    val graph = input1.zipWithIndex().map{case (x,i) => (i+1,(x.split(",").toList))}
      .map{case (x)=>
        val nums = ListBuffer[Int]()
          for (i<-0 to x._2.size-1){
            nums.append(x._2(i).toInt)
          }
        (x._1.toInt,nums)
      }
      .map{ case (x,y)=>(x,y.toList)}
      .collect().toList

    val map = graph.toMap
*/

    //graph
    val map = Map(1->List(2,4),2->List(1,3),3->List(2,4),4->List(1,3))

    def visit(graph:Map[Int,List[Int]], start:Int): List[List[Int]] =
    {
      def visit_1(nodes: List[Int], visited: List[List[Int]]): List[List[Int]]={

        //Gets the connected nodes that have not yet been visited that are attached
        //to the nodes contained in the List[Int] "nodes"
        val notVisited = nodes.flatMap(graph(_)).filterNot(visited.flatten.contains).distinct

        if (notVisited.isEmpty)
          visited
        else
        //Recursively call the same function using the not yet visited nodes, and passing
        //in the not yet visited nodes inside a growing list to store the result.
          visit_1(notVisited, notVisited::visited)
      }

      visit_1(List(start), List(List(start))).reverse
    }

    val result = visit(map,1)

    println(result.mkString(","))

  }

}
