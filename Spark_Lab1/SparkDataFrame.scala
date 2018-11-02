//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //Create a schema to fit the data in WorldCups.csv
    val schema = StructType{
      Seq(
        StructField("Year", IntegerType, true),
        StructField("Country", StringType, true),
        StructField("Winner", StringType, true),
        StructField("Second", StringType, true),
        StructField("Third", StringType, true),
        StructField("Fourth", StringType, true),
        StructField("Goals", IntegerType, true),
        StructField("Teams", IntegerType, true),
        StructField("Matches", IntegerType, true),
        StructField("Attendance", LongType, true)
      )
    }

    //Get the data in the csv, and separate the lines based on the end of line character.
    //  Also, drop the first row since the headers aren't necessary.
    val input1 = sc.textFile("E:\\School\\Hadoop Class\\Spark\\DF2\\SparkDataframe\\res\\WorldCups.csv").flatMap(x=>x.split("\n"))
        .mapPartitionsWithIndex{ (i, iter) => if (i==0) iter.drop(1) else iter }

    //From the input rdd, split the csv into separate values by the comma, and insert those values into a Row with type corrections.
    // The attendance numbers need to have the punctuation removed in order to process.
    val rowRdd = input1.map(_.split(","))
      .map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6).toInt,x(7).toInt,x(8).toInt,x(9).replaceAll("\\.","").toLong))

    //Apply the schema to the rowRdd
    val df = spark.createDataFrame(rowRdd,schema)

    df.show()

    //How many times has each country that has won the World Cup, won?
    df.groupBy("Winner").count().orderBy(desc("count")).show
    println(rowRdd.map(x=>(x(2),1))
      .reduceByKey(_+_)
      .sortBy(x=>x._2,false)
      .collect()
      .mkString("\n"))


    //Which host countries have won while hosting the World Cup?
    df.select("Country").where("Country = Winner").show
    println(rowRdd.map(x=>((x(1),x(0)),1))
      .union(rowRdd.map(x=>((x(2),x(0)),1)))
      .reduceByKey(_+_)
      .filter(x=>x._2>1)
      .map(x=>(x._1._1,x._1._2))
      .collect()
      .mkString("\n"))


    //Is there a correlation between goals scored and attendance numbers?
    println(df.stat.corr("Goals","Attendance"))

    //Is there also a correlation of attendance to time?
    println(df.stat.corr("Year","Attendance"))

    //How many times has the host country placed?
    val hostPlaces: Long = df.select("Country").where("Country = Winner")
      .union(df.select("Country").where("Country = Second"))
      .union(df.select("Country").where("Country = Third"))
      .union(df.select("Country").where("Country = Fourth"))
      .count()
    println(hostPlaces)
    println(rowRdd.map(x=>((x(1),x(0)),1))
      .union(rowRdd.map(x=>((x(2),x(0)),1)))
      .union(rowRdd.map(x=>((x(3),x(0)),1)))
      .union(rowRdd.map(x=>((x(4),x(0)),1)))
      .union(rowRdd.map(x=>((x(5),x(0)),1)))
      .reduceByKey(_+_)
      .filter(x=>x._2>1)
      .map(x=>(x._1._1,1))
      .reduceByKey(_+_)
      .map(x=>(1,x._2))
      .reduceByKey(_+_)
      .map(x=>x._2)
      .flatMap(x=>List(x))
      .collect()
      .mkString("\n"))

    //What percentage of World Cups has a host country placed?
    val totalCups: Long = df.select("Year").count()
    val percent: Double = hostPlaces.toDouble/totalCups.toDouble
    println(percent)
    val numerator = rowRdd.map(x=>((x(1),x(0)),1))
      .union(rowRdd.map(x=>((x(2),x(0)),1)))
      .union(rowRdd.map(x=>((x(3),x(0)),1)))
      .union(rowRdd.map(x=>((x(4),x(0)),1)))
      .union(rowRdd.map(x=>((x(5),x(0)),1)))
      .reduceByKey(_+_)
      .filter(x=>x._2>1)
      .map(x=>(x._1._1,1))
      .reduceByKey(_+_)
      .map(x=>(1,x._2))
      .reduceByKey(_+_)
      .map(x=>x._2)
      .flatMap(x=>List(x))
      .collect()
      .mkString("").toDouble
    val denominator = rowRdd.map(x=>(1,1))
        .reduceByKey(_+_)
        .map(x=>x._2)
        .flatMap(x=>List(x))
        .collect()
      .mkString("").toDouble
    println(numerator/denominator)


    //What is the average number of goals per match per year?
    df.select("Year", "Goals", "Matches").withColumn("Average",$"Goals"/$"Matches").show()
    println(rowRdd.map { case (x) =>
      val divide = x(6).toString.toDouble / x(8).toString.toDouble
      (x(0), divide)
    }
      .collect()
      .mkString("\n"))

    //How many times has each country, that has placed done so?
    df.select("Winner")
      .union(df.select("Second"))
      .union(df.select("Third"))
      .union(df.select("Fourth"))
      .groupBy("Winner")
      .count()
      .orderBy(desc("count"))
      .show()

    //How many World Cups had the correct number of teams for a proper tournament structure? ie. a power of 2
    println(df.select("Teams")
      .withColumn("log",log(2,"Teams").endsWith(".0"))
      .where("log=true")
      .count())

    //How many years has it been since the men's USA team has placed?
    df.select("Year", "Winner", "Second", "Third", "Fourth")
      .where("Winner like 'USA' OR Second like 'USA' OR Third like 'USA' OR Fourth like 'USA'")
      .drop("Winner","Second","Third","Fourth")
      .withColumn("Year",year(current_date())-max("Year"))
      .show()
  }
}

