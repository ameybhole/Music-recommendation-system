package nl.rug.sc.app

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
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
import scala.io.Codec
import scala.math.sqrt
import org.apache.log4j._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.spark_project.jetty.security.PropertyUserStore.UserListener


object StreamingMain extends App
{
  case class SongData1(newuserid: Double, newsongid: Double, listen_count: Double, tid: Int)

  val sparkSession = SparkSession // Usually you only create one Spark Session in your application, but for demo purpose we recreate them
    .builder()
    .appName("NetworkWordCount")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "35.204.169.60:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group2",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val sc =  ssc.sparkContext
  val topics = Array("new-topic")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  )


  val MongoMMF = MongoSpark.load(sparkSession, ReadConfig(Map("uri" -> "mongodb://35.204.169.60:27017/MyHist.MetaData")))
  //val newstream = stream.window(Seconds(20))

  stream.map(record => (record.key, record.value)).foreachRDD{rdd =>rdd.foreach
  {
    case(k, v) => println(k,v)
      //val dstream = stream.window(Seconds(20))
      val value = v.split(",")
      val a = value(0)
      val b = a.drop(1)
      val value1 = b.toDouble
      val value2 = value(1).toDouble
      val value3 = value(2).toDouble
      val value4 = value(3).dropRight(1).toInt

      val SongData = SongData1(value1,value2,value3,value4) // passing the values to case class songdata1


      val RddSongData = sc.parallelize(Array(SongData)) // rdd

      val dfSongData = sparkSession.createDataFrame(RddSongData) // dataframe
      println("printing our DATAFRAME")
      dfSongData.show()
      val samemodel = ALSModel.load("model")
      val predictions = samemodel.transform(dfSongData)
      //predictions.printSchema()
      predictions.show()
      //val userRecs = samemodel.recommendForAllUsers(10)
      //userRecs.show()
      //userRecs.printSchema()
      val ALS = predictions.na.fill(0.toFloat)
      //userRecs.filter(userRecs("newuserid")=== 148).show()


      val writeConfig = WriteConfig(Map("uri" -> "mongodb://35.204.169.60:27017", "database" -> "MyHist", "collection" -> "StreamingData"))
      MongoSpark.save(dfSongData, writeConfig)

      val MongoMDF = MongoSpark.load(sparkSession, ReadConfig(Map("uri" -> "mongodb://35.204.169.60:27017/MyHist.StreamingData")))
      //Convert DF to Dataset
      MongoMDF.printSchema()
      val counter = MongoMDF.count()
      println(MongoMDF.count())
     /* if (counter % 10 == 0 )
        {    val als = new ALS()
          .setMaxIter(5)
          .setRegParam(0.1)
          .setUserCol("newuserid")
          .setItemCol("newsongid")
          .setRatingCol("listen_count")

          val MongoHistDf = MongoSpark.load(sparkSession, ReadConfig(Map("uri" -> "mongodb://127.0.0.1/MyHist.Dataset")))
          val mongoHistDf1 = MongoHistDf.drop("song_id", "_id", "user_id")
          val mongostream = MongoMDF.drop("_id")
          println(mongoHistDf1.count())
          println(mongostream.count())
          val StreamHist = mongoHistDf1.union(mongostream)

          val updatedmodel = als.fit(StreamHist)

          updatedmodel.write.overwrite().save("model")

          val userRecsS = updatedmodel.recommendForAllUsers(20)
          userRecsS.show()
          //userRecsS.withColumn("temp", explode($"recommendations"))

          /*val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "MyHist", "collection" -> "StreamingRecommendatio"))
          MongoSpark.save(userRecs, writeConfig)*/

        }*/




      val popsongs = MongoMDF.groupBy("newsongid").agg(count("newuserid").as("usercount"),sum("listen_count").as("popularsong"))
      popsongs.show()


      // No of users listinging to
      /*val stat1 = MongoMDF.groupBy("listen_count").agg(count("newuserid").as("usercount"),count("newosongid").as("songcount"))
      stat1.show()*/

      //Top User
      val stat2 = MongoMDF.groupBy("newuserid").agg(count("newsongid").as("songcount"), sum("listen_count").as("total_listen_coutns"))
      stat2.show()

     /* val stat3 = MongoMDF.groupBy("newsongid").agg(count("newuserid").as("usercount"),mean("listen_count").as("songlistensum"))
      stat3.show()*/




  }

  }

  ssc.start()

  stream.start()

  ssc.awaitTermination()
}
