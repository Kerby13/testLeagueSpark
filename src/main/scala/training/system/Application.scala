package training.system

import java.io.{BufferedWriter, FileWriter}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source
import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try

import scala.util.Try


object Application extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("local-boot")

  val sc = new SparkContext(sparkConf)

  def timeCheck(time: String) = {
    val format = "yyyy-MM-dd HH:mm:ss"
    val df = new SimpleDateFormat(format)
    val tmp = df.format(time.toLong)
    if (((tmp.slice(11, 13).toInt - 3) >= 9) && ((tmp.slice(11, 13).toInt - 3) < 17))
      true
    else
      false
  }

  val first_stage = sc
    .textFile("./dataset/testLeague/1st_stage/*")
    .map(line => line.split("\\|", -1)).map(array => (array(1), array(2), array(32),0,0,0,0,0,0,0,0))

  .coalesce(1).saveAsTextFile("./output/test1")

/*
  val file = new File("./output/test_output")
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(first_stage.mkString("\n"))
  bw.close()*/

  sc.stop()
}