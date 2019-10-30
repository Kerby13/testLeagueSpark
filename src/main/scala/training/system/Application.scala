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

  def timeCheck1(time: String, activity_type: String): Integer = {
    if (activity_type.equals("V") && (time.slice(8, 10).toInt >= 6 && time.slice(8, 10).toInt < 11))
      1
    else if (activity_type.equals("V") && (time.slice(8, 10).toInt >= 11 && time.slice(8, 10).toInt < 19))
      2
    else if (activity_type.equals("V") && (time.slice(8, 10).toInt >= 19 && time.slice(8, 10).toInt < 23))
      3
    else if (activity_type.equals("V"))
      4
    else if (activity_type.equals("S") && (time.slice(8, 10).toInt >= 6 && time.slice(8, 10).toInt < 11))
      5
    else if (activity_type.equals("S") && (time.slice(8, 10).toInt >= 11 && time.slice(8, 10).toInt < 19))
      6
    else if (activity_type.equals("S") && (time.slice(8, 10).toInt >= 19 && time.slice(8, 10).toInt < 23))
      7
    else if (activity_type.equals("S"))
      8
    else -1
  }

  val first_stage = sc
    .textFile("./dataset/testLeague/1st_stage/*")
    .map(line => line.split("\\|", -1))
    .map(array => (array(1), array(2), array(32)))
    .map {
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 1 => ((phone, date.substring(0, 8)), (1, 0, 0, 0, 0, 0, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 2 => ((phone, date.substring(0, 8)), (0, 1, 0, 0, 0, 0, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 3 => ((phone, date.substring(0, 8)), (0, 0, 1, 0, 0, 0, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 4 => ((phone, date.substring(0, 8)), (0, 0, 0, 1, 0, 0, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 5 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 1, 0, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 6 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 1, 0, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 7 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 0, 1, 0))
      case (phone, date, activity_type) if timeCheck1(date, activity_type) == 8 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 0, 0, 1))
      //case (phone, date, activity_type) if timeCheck1(date, activity_type) == -1 => None
    }.reduceByKey {
    case ((v_morn_ac, v_day_ac, v_even_ac, v_night_ac, s_morn_ac, s_day_ac, s_even_ac, s_night_ac), (v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night))
    => (v_morn_ac + v_morn, v_day_ac + v_day, v_even_ac + v_even, v_night_ac + v_night, s_morn_ac + s_morn, s_day_ac + s_day, s_even_ac + s_even, s_night_ac + s_night)
  }
  .coalesce(1).saveAsTextFile("./output/test1")

  /*
    val file = new File("./output/test_output")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(first_stage.mkString("\n"))
    bw.close()*/

  sc.stop()
}