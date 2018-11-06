import StreamFile.updateFunction
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\winutils")

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val data = ssc.socketTextStream("localhost",9999)
    ssc.checkpoint("E:\\log")
    //val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    //val words = lines.flatMap(_.split(" "))
    //val wc = data.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey[Int](updateFunction _)
    val wc = data.flatMap(_.split("")).map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey[Int](updateFunction _)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var result: Option[(Int)] = null
    if(newValues.isEmpty){ //check if the key is present in new batch if not then return the old values
      result=Some(runningCount.get)
    }
    else{
      newValues.foreach { x => {// if we have keys in new batch ,iterate over them and add it
        if(runningCount.isEmpty){
          result=Some(x)// if no previous value return the new one
        }else{
          result=Some(x+runningCount.get) // update and return the value
        }
      } }
    }
    result
  }

}

