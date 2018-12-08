import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val stations = spark.read.option("header", "true").csv("C:\\Users\\Dave\\Downloads\\201508_station_data.csv")
    val trips = spark.read.option("header", "true").csv("C:\\Users\\Dave\\Downloads\\201508_trip_data.csv")



/*
    val input = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    val output = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")
*/

    /*
    g.vertices.show(10)
    g.edges.show(10)
    g.edges.groupBy("dst").count().show()
    g.edges.groupBy("src").count().show()
    */

    val input = stations.select("station_id", "name")
      .distinct()
      .withColumnRenamed("station_id","id")
    val output = trips.select("Start Terminal", "End Terminal", "Duration")
      .distinct()
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")

    val g = GraphFrame(input,output)

    val results = g.triangleCount.run()
    results.select("id","count").show()
    //save to parquet

    /*
    //val input = stations.select("station_id", "name")
    val input = stations.select("station_id", "landmark")
      //.distinct()
      .withColumnRenamed("station_id","id")
    val output = trips.select("Trip ID", "Duration", "Start Terminal", "End Terminal")
      .distinct()
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")

    val g = GraphFrame(input, output)

    val paths = g.bfs.fromExpr("landmark = 'Palo Alto'").toExpr("landmark = 'Mountain View'").maxPathLength(2).run()
    paths.show()
    */

/*
    val ranks = g.pageRank.resetProbability(0.15).maxIter(1).run()
    ranks.vertices.select("id","pagerank").show()
    ranks.edges.select("src","dst","weight").show()
*/
  }
}
