import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("C:\\Users\\Dave\\Downloads\\ConsumerComplaints.csv")

    val input = spark.sparkContext.textFile("C:\\Users\\Dave\\Downloads\\ConsumerComplaints.csv").flatMap(x=>x.split("\n"))
      .map(x=>x.split(","))
      .toDF()
      /*.map{ case (y) =>
        if (!y(9).equals("Zip Code")){
          new Complaint(y(0),y(1),y(2),y(3),y(4),y(5),y(6),y(7),y(8),y(9).toInt,y(10),y(11),y(12),y(13),y(14),y(15),
            y(16),y(17).toInt)
        }else{
          new Complaint("0","0","0","0","0","0","0","0","0",0,"0","0","0","0","0","0","0",0)
        }
      }
        .toDF()
*/


    //df.write.json("E:\\School\\Hadoop Class\\Spark\\output.json")

    //Subtract the number of distinct rows from the total number of rows to find the repeats
    //Result is 88
    println(df.count() - df.distinct().count())

    val debtCol = df.filter(df("Product name") === "Debt collection")
    val mortgage = df.filter(df("Product name") === "Mortgage")

    val debtAndMort = debtCol.union(mortgage).orderBy("Company")

    println("Debt: " + debtCol.count())       //16106
    println("Mortgage: " + mortgage.count())  //18734
    println("Union: " + debtAndMort.count())  //34840
    debtAndMort.show(10)

    /*

    +-------------+---------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+-------+----------+--------+----+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+
|Date Received|   Product Name|         Sub Product|               Issue|           Sub Issue|Consumer Complaint Narrative|Company Public Response|Company|State Name|Zip Code|Tags|Consumer Consent Provided|Submitted via|Date Sent to Company|Company Response to Consumer|Timely Response|Consumer Disputed|Complaint ID|
+-------------+---------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+-------+----------+--------+----+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+
|   2015-03-24|Debt collection|       I do not know|Cont'd attempts c...|    Debt is not mine|        This company cont...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-19|Debt collection|       I do not know|Cont'd attempts c...|    Debt is not mine|        "Reached out to t...|    consent and/or a...|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-24|Debt collection|         Payday loan|False statements ...|Impersonated an a...|        A man named XXXX ...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-23|Debt collection|       I do not know|Disclosure verifi...|Not given enough ...|        "I recieved a not...|    XXXX. and that t...|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-19|Debt collection|Other (i.e. phone...|Cont'd attempts c...|       Debt was paid|        XXXX has claimed ...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-23|Debt collection|Other (i.e. phone...|Improper contact ...|Contacted me afte...|        I do n't know how...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-19|Debt collection|             Medical|Cont'd attempts c...|    Debt is not mine|        This dentist appt...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-24|Debt collection|         Credit card|Communication tac...|Frequent or repea...|        I 've been receiv...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-24|Debt collection|       I do not know|Cont'd attempts c...|    Debt is not mine|        This company cont...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
|   2015-03-24|Debt collection|                Auto|False statements ...|Attempted to coll...|        I am a victim of ...|                   null|   null|      null|    null|null|                     null|         null|                null|                        null|           null|             null|        null|
+-------------+---------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+-------+----------+--------+----+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+

     */

    df.groupBy("Zip Code").count().show()

    /*
    +--------+-----+
|Zip Code|count|
+--------+-----+
|   36526|    8|
|   19132|    9|
|   28117|    8|
|   94102|    7|
|   80047|    3|
|   75007|   24|
|   49079|    2|
|   95134|    8|
|   18509|    3|
|   30002|    7|
|    4937|    3|
|   14204|    2|
|   29210|    8|
|   77339|    7|
|   45157|    3|
|    2136|   20|
|   33174|    3|
|   32812|    8|
|    7762|    3|
|   39641|    1|
+--------+-----+
only showing top 20 rows
     */

    val joinTable = df.join(df.groupBy("Company").count(),"Company")
    joinTable.show()
    /*
    +--------------------+-------------+--------------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+----------+--------+--------------+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+-----+
|             Company|Date Received|        Product Name|         Sub Product|               Issue|           Sub Issue|Consumer Complaint Narrative|Company Public Response|State Name|Zip Code|          Tags|Consumer Consent Provided|Submitted via|Date Sent to Company|Company Response to Consumer|Timely Response|Consumer Disputed|Complaint ID|count|
+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+----------+--------+--------------+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+-----+
|Wells Fargo & Com...|   2013-07-29|       Consumer Loan|        Vehicle loan|Managing the loan...|                null|                        null|                   null|        VA|   24540|          null|                      N/A|        Phone|          2013-07-30|        Closed with expla...|            Yes|               No|      468882| 3987|
|Wells Fargo & Com...|   2013-07-29|Bank account or s...|    Checking account|Using a debit or ...|                null|                        null|                   null|        CA|   95992|Older American|                      N/A|          Web|          2013-07-31|        Closed with expla...|            Yes|               No|      468889| 3987|
|   Santander Bank US|   2013-07-29|Bank account or s...|    Checking account|Account opening, ...|                null|                        null|                   null|        NY|   10065|          null|                      N/A|          Fax|          2013-07-31|                      Closed|            Yes|               No|      468879|  201|
|Wells Fargo & Com...|   2013-07-29|Bank account or s...|    Checking account|Deposits and with...|                null|                        null|                   null|        GA|   30084|          null|                      N/A|          Web|          2013-07-30|        Closed with expla...|            Yes|               No|      468949| 3987|
|Franklin Credit M...|   2013-07-29|            Mortgage|Conventional fixe...|Loan servicing, p...|                null|                        null|                   null|        CT|    6106|          null|                      N/A|          Web|          2013-07-30|        Closed with expla...|            Yes|               No|      475823|   11|
|     Bank of America|   2013-07-29|Bank account or s...|    Checking account|Deposits and with...|                null|                        null|                   null|        TX|   75025|          null|                      N/A|          Web|          2013-07-30|        Closed with expla...|            Yes|               No|      468981| 4642|
|      NRA Group, LLC|   2013-07-29|     Debt collection|Other (i.e. phone...|Cont'd attempts c...|    Debt is not mine|                        null|                   null|        VA|   20147|          null|                      N/A|          Web|          2013-08-07|        Closed with non-m...|            Yes|               No|      467801|   60|
|SunTrust Banks, Inc.|   2013-07-29|     Debt collection|       I do not know|Cont'd attempts c...|       Debt was paid|                        null|                   null|        FL|   32818|          null|                      N/A|     Referral|          2013-08-01|        Closed with expla...|            Yes|              Yes|      475728|  462|
|            Citibank|   2013-07-29|         Credit card|                null|   Billing statement|                null|                        null|                   null|        OH|   45247|          null|                      N/A|     Referral|          2013-07-30|        Closed with expla...|            Yes|              Yes|      469026| 2575|
|Wells Fargo & Com...|   2013-07-29|            Mortgage|      Other mortgage|Loan servicing, p...|                null|                        null|                   null|        NV|   89511|          null|                      N/A|     Referral|          2013-07-30|        Closed with expla...|            Yes|              Yes|      469035| 3987|
|     Bank of America|   2013-07-29|            Mortgage|      Other mortgage|Loan modification...|                null|                        null|                   null|        NC|   27949|          null|                      N/A|     Referral|          2013-07-30|        Closed with non-m...|            Yes|               No|      469037| 4642|
|JPMorgan Chase & Co.|   2013-07-29|            Mortgage|      Other mortgage|Loan servicing, p...|                null|                        null|                   null|        CA|   90703|          null|                      N/A|     Referral|          2013-07-30|        Closed with expla...|            Yes|               No|      469284| 3295|
|            Citibank|   2013-07-29|            Mortgage|      Other mortgage|Loan modification...|                null|                        null|                   null|        CA|   95821|          null|                      N/A|     Referral|          2013-07-31|        Closed with expla...|            Yes|              Yes|      480488| 2575|
|Asset Management ...|   2013-07-29|     Debt collection|         Credit card|Communication tac...|Frequent or repea...|                        null|                   null|        PA|   19145|Older American|                      N/A|        Phone|          2013-08-01|        Closed with expla...|             No|             null|      468904|    8|
|Wells Fargo & Com...|   2013-07-29|     Debt collection|         Credit card|Communication tac...|Frequent or repea...|                        null|                   null|        PA|   19145|Older American|                      N/A|        Phone|          2013-08-01|        Closed with non-m...|            Yes|               No|      468905| 3987|
|JPMorgan Chase & Co.|   2013-07-29|            Mortgage|Conventional fixe...|Loan modification...|                null|                        null|                   null|        NY|   14092|          null|                      N/A|        Phone|          2013-07-31|        Closed with expla...|            Yes|               No|      469057| 3295|
|               Ocwen|   2013-07-29|            Mortgage|Conventional fixe...|Loan modification...|                null|                        null|                   null|        FL|   33426|          null|                      N/A|          Web|          2013-07-31|        Closed with expla...|            Yes|              Yes|      469060| 2483|
|JPMorgan Chase & Co.|   2013-07-29|            Mortgage|Conventional fixe...|Loan modification...|                null|                        null|                   null|        NY|   10019|          null|                      N/A|          Web|          2013-07-30|        Closed with expla...|            Yes|              Yes|      469070| 3295|
|            Citibank|   2013-07-29|            Mortgage|Conventional fixe...|Loan servicing, p...|                null|                        null|                   null|        NJ|    7604|          null|                      N/A|          Web|          2013-07-30|        Closed with expla...|            Yes|               No|      472236| 2575|
| Synchrony Financial|   2013-07-29|         Credit card|                null|APR or interest rate|                null|                        null|                   null|        WA|   98548|          null|                      N/A|          Web|          2013-07-29|        Closed with monet...|            Yes|               No|      469131| 1132|
+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+----------------------------+-----------------------+----------+--------+--------------+-------------------------+-------------+--------------------+----------------------------+---------------+-----------------+------------+-----+
only showing top 20 rows
     */

    val pctByCompany = joinTable.select(joinTable("Company"),joinTable("Count")/joinTable.count()).distinct()
    pctByCompany.show()

    /*
    +--------------------+--------------------+
|             Company|     (Count / 64963)|
+--------------------+--------------------+
|LCS Financial Cor...|7.696688884441914E-5|
|OneMain Financial...|0.001462370888043...|
|              Nelnet|1.847205332266059...|
|Northstar Locatio...|2.462940443021412E-4|
|Deca Financial Se...|7.696688884441914E-5|
|Financial Asset R...|7.696688884441914E-5|
|Anselmo Lindberg ...|1.539337776888382...|
|United Recovery S...|4.618013330665148E-5|
|Student Assistanc...|4.618013330665148E-5|
|Asset Maximizatio...|1.539337776888382...|
|First Direct Lend...|1.539337776888382...|
|  is a false witness|1.539337776888382...|
|    CIR, Law Offices|7.696688884441914E-5|
|Brinkman Alliance...| 6.15735110755353E-5|
|  Brown Olcott, PLLC|3.078675553776765E-5|
|Guardian Mortgage...|1.539337776888382...|
|Duncan Solutions,...|4.618013330665148E-5|
|Mark A. Kirkorsky...|1.539337776888382...|
|    Freedom Mortgage|6.927019995997722E-4|
|North American As...|1.385403999199544...|
+--------------------+--------------------+
only showing top 20 rows

     */

    println(df.take(13).last)

    /*
    [2013-07-29,Mortgage,Other mortgage,Loan modification,collection,foreclosure,null,null,null,Citibank,CA,95821,null,N/A,Referral,2013-07-31,Closed with explanation,Yes,Yes,480488]

     */

    input.show()
  }

  case class Complaint(dateRcvd: String, productName: String, subProduct: String,
                       issue: String, subIssue: String, narrative: String,
                       publicResponse: String, company: String, state: String,
                       zipCode: Int, tags: String, consent: String,
                       submittedVia: String, dateToCompany: String,
                       responseToConsumer: String, timely: String, disputed: String,
                       id: Int)
}

