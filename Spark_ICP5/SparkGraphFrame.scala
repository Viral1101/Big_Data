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

    val input = stations.select("name", "lat", "long", "landmark")
      .distinct()
      .withColumnRenamed("name","id")
    val output = trips.select("Start Station", "End Station", "Duration")
      .distinct()
      .withColumnRenamed("Start Station","src")
      .withColumnRenamed("End Station","dst")

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
    val g = GraphFrame(input,output)
    g.vertices.show(10)
    g.edges.show(10)
    g.edges.groupBy("dst").count().show()
    g.edges.groupBy("src").count().show()
  }
}
