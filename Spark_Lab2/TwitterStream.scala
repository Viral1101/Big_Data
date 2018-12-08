import org.apache.spark.SparkConf
//import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStream {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")
    // Create the context with a 1 second batch size
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("twitterinformatics2")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("E:\\log")


    //accept text filters from the arguments
    val filters = args.take(args.length)

    //get a twitter stream using the text filters, indicated to use the properties file for credentials with None
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //flat map tweets into individual words and count them, keep a running tally with updateStateByKey
    val tweetWordCount = stream.flatMap(status=>status.getText.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
      .updateStateByKey[Int](updateFunction _)

    tweetWordCount.print()



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
