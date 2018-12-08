import org.apache.spark.SparkConf
//import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStream {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\winutils")
    // Create the context with a 1 second batch size
    val conf = new SparkConf()
      //.set("cassandra.connection.host","127.0.0.1:9042")
      .setMaster("local[2]")
      .setAppName("twitterinformatics2")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("E:\\log")

    val filters = args.take(args.length)

    val stream = TwitterUtils.createStream(ssc, None, filters)

    //stream.saveAsTextFiles("tweets","json")

    val tweetWordCount = stream.flatMap(status=>status.getText.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
      .updateStateByKey[Int](updateFunction _)

    tweetWordCount.print()


    //val stopList :List[String]= ssc.sparkContext.textFile("src/main/resource/NLTK_English_Stop_Word_Corpus").collect().toList
    //val stopWordsList = ssc.sparkContext.broadcast(stopList)
    //val naiveBayesModel : NaiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, "data/tweets_sentiment/NBModel")
/*
    val data = stream.map{status =>

      val sentiment = CoreNLPSentimentAnalyzer.sentiment(status.getText)
      val avgSent = sentiment.reduce(_+_)/sentiment.length

      //val tweetInWords: Seq[String] = getBarebonesTweetText(status.getText, stopList)
      //val bayesScore = naiveBayesModel.predict(transformFeatures(tweetInWords))
      val bayesScore = 2

      (status.getId, avgSent, bayesScore, status.getGeoLocation, status.getLang, status.getUser.getLocation, status.getText)
    }

    data.foreachRDD{rdd=>
      if (rdd.count() > 0) {
        rdd.saveToCassandra("tweets","tweet")
      }
    }

    data.print()

    //data.saveAsTextFiles("sentiment","json")
*/
    ssc.start()
    ssc.awaitTermination()
  }
/*
  val hashingTF = new HashingTF()

  /**
    * Transforms features to Vectors.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return Vector
    */
  def transformFeatures(tweetText: Seq[String])= {
    hashingTF.transform(tweetText)
  }
  */
/*
  def getBarebonesTweetText(tweetText: String, stopWordsList: List[String]): Seq[String] = {
    //Remove URLs, RT, MT and other redundant chars / strings from the tweets.
    tweetText.toLowerCase()
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
      .filter(!stopWordsList.contains(_))
    //.fold("")((a,b) => a.trim + " " + b.trim).trim
  }
*/
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
