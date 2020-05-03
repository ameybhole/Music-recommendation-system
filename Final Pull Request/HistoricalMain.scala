package nl.rugds.app

import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import nl.rugds.app.HistoricalMain.sparkSession
import nl.rugds.app.old.MongoSparkMain.sparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.StringIndexer
import com.mongodb.spark.sql.DefaultSource
import org.apache.spark.sql.expressions.Window
import scala.io.Codec
import scala.math.sqrt
import org.apache.log4j._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.spark_project.jetty.security.PropertyUserStore.UserListener

object HistoricalMain extends App {

  /////////////////////////////////DATA LOADING AND PRE-PROCESSING//////////////////////////////////////////////////////////////////////////////////////////
  case class SongData(tid: Int, user_id: String, song_id: String, listen_count: Double)
  case class IndexSongData(tid: Int, user_id: String, song_id: String, listen_count: Double, newuserid: Double, newsongid: Double)
  case class SongMetaData(mid: Int, song_id: String, title: String, release: String, artist_name: String, year: Int)
  case class IndexSongMetaData(mid: Int, song_id: String, title: String, release: String, artist_name: String, year: Int, newsongid: Double)


  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  //Build Spark session
  val sparkSession = SparkSession.builder()
    .appName("Addition")
    .master("local[*]")
    .getOrCreate()

  //Schema encoder
  val SDschema = Encoders.product[SongData].schema
  val SMDschema = Encoders.product[SongMetaData].schema
  val ISDschema = Encoders.product[IndexSongData].schema
  val ISMDschema = Encoders.product[IndexSongMetaData].schema

  // Spark session
  val sc = sparkSession.sparkContext
  import sparkSession.implicits._

 /*   // Get Dataset csv into MongoDB
    val path1 = "/Users/marios/Desktop/Project/src/main/resources/Dataset.csv"
    val Dataset = sparkSession.read
      .option("header", "true")
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("partitioner", "spark.mongodb.input.partitionerOptions.MongoPaginateBySizePartitioner ")
      .schema(SDschema)
      .csv(path1)
      .as[SongData]

     //Get MetaData csv into MongoDB
      val path2 = "/Users/marios/Desktop/Project/src/main/resources/MetaData.csv"
      val Metadata = sparkSession.read
        .option("header", "true")
        .format("com.mongodb.spark.sql.DefaultSource")
        .option("partitioner", "spark.mongodb.input.partitionerOptions.MongoPaginateBySizePartitioner ")
        .schema(SMDschema)
        .csv(path2)
        .as[SongMetaData]

      //Check Dataset schema
      Dataset.printSchema()

      //Check MetaData Schema
      Metadata.printSchema()

      //Dataset String Indexer
      val stringindexer = new StringIndexer()
        .setInputCol("user_id")
        .setOutputCol("newuserid")
      val modelu = stringindexer.fit(Dataset)
      val Dataset1 = modelu.transform(Dataset)

      val stringindexer1 = new StringIndexer()
        .setInputCol("song_id")
        .setOutputCol("newsongid")
      val models = stringindexer1.fit(Dataset1)
      val Dataset2 = models.transform(Dataset1)

      //Metadata String Indexer
      val stringindexer2 = new StringIndexer()
        .setInputCol("song_id")
        .setOutputCol("newsongid")
      val modelms = stringindexer2.fit(Metadata)
      val Metadata1 = modelms.transform(Metadata)

      //Check Indexed dataset schema
      Dataset2.printSchema()

      //Check Indexed Metadata schema
      Metadata1.printSchema()
      println("askdal")
      //Write Dataset into mongodb
      val writeConfig = WriteConfig(Map("uri" -> "mongodb://35.204.169.60:27017/", "database" -> "MyHist", "collection" -> "Dataset"))
      MongoSpark.save(Dataset2, writeConfig)

      //Write Metadata into mongodb
      val writeConfig1 = WriteConfig(Map("uri" -> "mongodb://35.204.169.60:27017/", "database" -> "MyHist", "collection" -> "MetaData"))
      MongoSpark.save(Metadata1, writeConfig1)
*/
  //Read Dataset from mongodb
  val MongoDF = MongoSpark.load(sparkSession, ReadConfig(Map("uri" -> "mongodb://35.204.169.60:27017/MyHist.Dataset")))
  //Convert DF to Dataset
  val MongoDS = MongoDF.as[IndexSongData]
  println("dataset ready")
  //Read MetaData from mongodb
  val MongoMDF = MongoSpark.load(sparkSession, ReadConfig(Map("uri" -> "mongodb://35.204.169.60:27017/MyHist.MetaData")))
  //Convert DF to Dataset
  val MongoMDS = MongoMDF.as[IndexSongMetaData]

  ////////////////////////////////TRAINING AND TESTING ALS MODEL//////////////////////////////////////////////////////////////////////////////////////////


  //Split in Training and Testing
  val Array(training, test) = MongoDS.randomSplit(Array(0.8, 0.2))

  //Build AlS Model
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.1)
    .setUserCol("newuserid")
    .setItemCol("newsongid")
    .setRatingCol("listen_count")
  val model = als.fit(training)
  model.write.overwrite().save("model")

  // Evaluate the model
  val predictions = model.transform(test)
  //Fill NaNs with 0

  val ALS = predictions.na.fill(0.toFloat)
  //Print no of NaNs
  println(ALS.filter(ALS("prediction").isNull || ALS("prediction") === "" || ALS("prediction").isNaN).count())
  println(ALS.count())
  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("listen_count")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(ALS)
  println(s"Root-mean-square error = $rmse")
  //model.save("D:\\Amey\\Masters\\Semester2a\\ScalableComputing")

  //Generate top 10 songs recommendations for each user
  val userRecs = model.recommendForAllUsers(20)
  //userRecs.show(50)
  userRecs.printSchema()
  //println(userRecs.count())
  val ALSuser = userRecs.withColumn("recommendations", explode($"recommendations"))
  ALSuser.printSchema()
  //println(ALSuser.count())
  val ALSuserfinal = ALSuser.withColumn("newsongid", $"recommendations.newsongid").withColumn("predictions", $"recommendations.rating")
  val ALSuserfinalClean = ALSuserfinal.na.fill(0.toFloat)
  //Print no of NaNs
  println(ALSuserfinalClean.filter(ALSuserfinalClean("predictions").isNull || ALSuserfinalClean("predictions") === "" || ALSuserfinalClean("predictions").isNaN).count())
  //println(ALSuserfinalClean.count())
  ALSuserfinalClean.show()
  ALSuserfinalClean.printSchema()


  // Generate top 10 user recommendations for each song
  //val songRecs = model.recommendForAllItems(10)
  //songRecs.show(50)


  /////////////////////////////////GLOBAL BASELINE//////////////////////////////////////////////////////////////////////////////////////////

  //Calculate the Mean across all Users and Songs
  //no of rows
  println(MongoDS.count())
  //Calculate sum of listen counts using rdds
  val listensRDD = MongoDS.select(col("listen_count")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
  println(listensRDD)
  //Calculate sum of listens counts using dataset
  val sumSteps =  MongoDS.agg(sum("listen_count")).first.get(0)
  println(sumSteps)

  //Mean using the listen counts for rdds
  val mean = listensRDD/MongoDS.count()
  println(mean)

  //For all songs find mean above or below the mean
  val SongAvgDiff = MongoDS.groupBy("newsongid").agg(count("newuserid").as("usercount"),sum("listen_count").as("songlistensum")).withColumn("songavg",$"songlistensum"/$"usercount").withColumn("avgsongdiff",$"songavg"-mean)
  SongAvgDiff.show()
  val SongAD = SongAvgDiff.drop("usercount", "songlistensum")



  //For all users find mean above or below the mean
  val UserAvgDiff = MongoDS.groupBy("newuserid").agg(count("newsongid").as("songcount"),sum("listen_count").as("userlistensum")).withColumn("useravg",$"userlistensum"/$"songcount").withColumn("avguserdiff",$"useravg"-mean)
  UserAvgDiff.sort("newuserid").show()
  val UserAD = UserAvgDiff.drop("songcount", "userlistensum")


  //val newDf = MongoDS.join(Sad,MongoDS("newsongid") === Sad("newsongid"))
  //val MongoDS1 =  MongoDS.join(SongAD,"newsongid","inner")
  val MongoDS1 = ALSuserfinalClean.join(SongAD,Seq("newsongid"),joinType = "inner")
  val ALSGB = MongoDS1.join(UserAD, Seq("newuserid"), joinType = "inner").withColumn("GlobalBaseline",$"avgsongdiff" + $"avguserdiff" + mean).withColumn("FinalPredictions1",$"GlobalBaseline" + $"predictions").withColumn("ALSavgdiff", $"predictions" - $"useravg").withColumn("FinalPredictions2",$"GlobalBaseline" + $"ALSavgdiff")
  ALSGB.show()
  val ALSGBnew = ALSGB.drop("recommendations","predictions","songavg", "avgsongdiff","useravg", "avguserdiff", "GlobalBaseline","ALSavgdiff")
  /*case class ALSGD(newuserid: Double,newsongid: Double,FinalPredicitions1:Double, FinalPredictions2: Double )
  val finalDS = ALSGBnew.as[ALSGD]
  val finalDS1 = finalDS.groupBy("newuserid")
  finalDS.printSchema()
finalDS.show()
  val windowSpec = Window.partitionBy(ALSGBnew("newuserid")).orderBy(desc("FinalPredictions2"))
  ALSGBnew.withColumn("rank",rank().over(windowSpec)).show(3000)
  val Results = ALSGBnew.withColumn("rank",rank().over(windowSpec)).filter(col("rank") <=3)
  println(Results.count())
  println(Results.groupBy("newuserid"))
  Results.groupBy("newuserid")
  val distinctsongids = Results.select("newsongid").distinct().as[Double].toDF()
  distinctsongids.printSchema()
  distinctsongids.count()
  MongoMDS.join(distinctsongids,Seq("newsongid"), joinType = "Inner").show()
  val JoinDS = distinctsongids.join(MongoMDS,Seq("newsongid"), joinType = "Inner")
  println(JoinDS.count())
  JoinDS.printSchema()*/


  // val asdf = ALSGBnew.orderBy(ALSGBnew("FinalPredictions2").over(windowSpec))
  //a.show(10000)
  //println(a.count())



  /*println(ALSGB.orderBy(desc("FinalPredictions2")).groupBy("newuserid"))
  val as = ALSGB.groupBy("newuserid")*/

  //ALS.show()
  //  ALSGB.show()
  //println(ALSGB.count())
  /*

      //ALSGB.filter(ALSGB("newuserid") === 299.0).orderBy(desc("FinalPredictions2")).show(10)
    val distinctValuesDF = ALSGB.select(ALSGB("newuserid")).distinct
   println("DISTINCT DATASET")
    distinctValuesDF.show(5)
   val i =0
    val counter = distinctValuesDF.count()
    val ar = distinctValuesDF.select("newuserid").rdd.map(r=>r(0)).collect()
    println("printing the third value of our array")
  val counter1 = counter.toInt
    println(ar(2))

    for (i<-1 to counter1)
    {
      println("these are the top 10 recommendations for each user")
      ALSGB.filter(ALSGB("newuserid") === ar(i)).orderBy(desc("FinalPredictions2")).show(10)
    }*/

  // distinctValuesDF.foreach(x=>  (ALSGB.filter(ALSGB("newuserid") === x).orderBy(desc("FinalPredictions2")).toDF))

  /*
    val Results =  ALSGB.join(MongoMDS, Seq("newsongid"), joinType = "outer")
    println(Results.count())
    Results.show()

     val GB1 = GB.drop("newuserid", "newsongid", "_id", "listen_count", "song_id", "user_id")
    val ALSGB = GB1.join(ALS, Seq("tid"), joinType = "outer").withColumn("FinalPredictions",$"GlobalBaseline" + $"prediction")
    println(ALSGB.count())
    ALSGB.show()*/
}