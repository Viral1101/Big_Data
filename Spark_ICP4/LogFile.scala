import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets


object LogFile {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\winutils")

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    val lines = sc.textFile("E:\\School\\Hadoop Class\\SparkStreamingScala\\src\\main\\scala\\lorem.txt").flatMap(x=>x.split("\n")).zipWithIndex().sortBy(x=>x._2,false)
    val lineCount = lines.first()._2

    for(i<-1 to 30){
      var rand: Int = (Math.random()*(lineCount-1)).toInt + 1
      val output = lines.take(rand)(rand-1)._1

      Files.write(Paths.get("log_"+i+".txt"), output.getBytes(StandardCharsets.UTF_8))

      Thread.sleep(1000)
    }
  }
}
