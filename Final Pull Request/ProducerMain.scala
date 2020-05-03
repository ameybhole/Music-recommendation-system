package nl.rugds.app.old

import java.util.{Date, Properties}
import java.util.{Date, Properties}
import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import nl.rugds.app.HistoricalMain.{IndexSongData, IndexSongMetaData, SongData, SongMetaData, sparkSession, _}
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

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Codec
import scala.math.sqrt
import org.apache.log4j._
import org.spark_project.jetty.security.PropertyUserStore.UserListener
import com.mongodb.spark._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.config.ReadConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.Encoders

import scala.util.Random


object ProducerMain extends App {

  case class SongData(tid: Int, user_id: String, song_id: String, listen_count: Double)

  case class IndexSongData(tid: String, user_id: String, song_id: String, listen_count: String, newuserid: String, newsongid: String)

  case class SongMetaData(mid: Int, song_id: String, title: String, release: String, artist_name: String, year: Int)

  case class IndexSongMetaData(mid: Int, song_id: String, title: String, release: String, artist_name: String, year: Int, newsongid: Int)


  //Schema encoder
  val SDschema = Encoders.product[SongData].schema
  val SMDschema = Encoders.product[SongMetaData].schema
  val ISDschema = Encoders.product[IndexSongData].schema
  val ISMDschema = Encoders.product[IndexSongMetaData].schema


  val events = 1
  val topic = "new-topic"
  val brokers = "35.204.169.60:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val sparkSession = SparkSession.builder()
    .appName("Addition")
    .master("local[*]")
    .config("spark.mongodb.input.uri", "mongodb://35.204.169.60:27017/MyHist.Dataset")
    .config("spark.mongodb.output.uri", "mongodb://35.204.169.60:27017/mydb.results")
    .getOrCreate()

  import sparkSession.implicits._

  val sc = sparkSession.sparkContext
  val mongoDF = MongoSpark.load(sparkSession)

  val mongoDS = mongoDF.as[IndexSongData]




  mongoDS.printSchema()
  val asdf = mongoDS.map(x => x.tid)

  asdf.show(2)

  val asdf2 = mongoDS.map(x => (x.newuserid, x.newsongid, x.listen_count, x.tid))
  val check = asdf2.take(10000)

  // println("asdf2 values")

  //  asdf2.show(20)
  // println("cheeeeeeck")
  //check.foreach(println)

  val split = check.mkString(",")



  //  val producer = new KafkaProducer[String, String](props)
  //  check.foreach { song =>
  //
  //    val a = Array(song)
  //    val sp = a.mkString(",")
  //    val data1 = new ProducerRecord[String, String]("asdas", "SIXSIXSIX")
  //    val result = producer.send(data1)
  //
  //  }
  //  check.foreach { song =>
  //    val a = Array(song)
  //    val sp = a.mkString(",")
  //    println(sp)
  //  }

  //  wordCounts.foreachRDD({ rdd =>
  //    import spark.implicits._
  //    val wordCounts = rdd.map({ case (word: String, count: Int)
  //    => WordCount(word, count) }).toDF()
  //
  //    wordCounts.write.mode("append").mongo()
  //  })




  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  // for (nEvents <- Range(0, events)) {


  //  asdf2.foreach{row =>
  //    val b = Array(row)
  //    val splat = b.mkString(",")



  check.foreach { song =>
    val a = Array(song)
    val sp = a.mkString(",")


    val runtime = new Date().getTime()
    //  val ip = "192.168.2." + rnd.nextInt(255)
    //  val msg = runtime + "," + nEvents + ",www.example.com," + ip
    val data = new ProducerRecord[String, String](topic, "sixsixsix", sp)


    //async
    //producer.send(data, (m,e) => {})
    //sync
    val result = producer.send(data)
  }
  // println(data)
  // }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()

}