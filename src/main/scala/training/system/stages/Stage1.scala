package training.system.stages

import org.apache.spark.sql.functions.split
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.io.Path

class Stage1 extends Serializable {
  private val sparkSession: SparkSession = SparkSession.builder.getOrCreate()

  import sparkSession.implicits._

  def timeCheck(time: String, activity_type: String): Integer = {
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

  def load(sc: SparkContext): DataFrame = {
    val rdd = sc
      .textFile(Paths.path_stage1)
      .map(line => line.split("\\|", -1))
      .map(array => (array(1), array(2), array(32)))
      .map {
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 1 => ((phone, date.substring(0, 8)), (1, 0, 0, 0, 0, 0, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 2 => ((phone, date.substring(0, 8)), (0, 1, 0, 0, 0, 0, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 3 => ((phone, date.substring(0, 8)), (0, 0, 1, 0, 0, 0, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 4 => ((phone, date.substring(0, 8)), (0, 0, 0, 1, 0, 0, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 5 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 1, 0, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 6 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 1, 0, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 7 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 0, 1, 0))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == 8 => ((phone, date.substring(0, 8)), (0, 0, 0, 0, 0, 0, 0, 1))
        case (phone, date, activity_type) if timeCheck(date, activity_type) == -1 => (("wrong_activity_type", "-1"), (0, 0, 0, 0, 0, 0, 0, 1))
      }.filter {
      case ((phone, date), activities) if phone == "wrong_activity_type" => false
      case _ => true
    }.map {
      case ((phone, date), (cm, cd, ce, cn, sm, sd, se, sn)) => Row(phone, date, cm, cd, ce, cn, sm, sd, se, sn)
    }

    val cdrStruct = StructType(
      StructField("phone", StringType, nullable = true) ::
        StructField("date", StringType, nullable = true) ::
        StructField("cm", IntegerType) ::
        StructField("cd", IntegerType) ::
        StructField("ce", IntegerType) ::
        StructField("cn", IntegerType) ::
        StructField("sm", IntegerType) ::
        StructField("sd", IntegerType) ::
        StructField("se", IntegerType) ::
        StructField("sn", IntegerType) :: Nil
    )
    sparkSession.createDataFrame(rdd, cdrStruct).createOrReplaceTempView("cdrView")
    sparkSession.table("cdrView").groupBy("phone", "date").sum()
  }
}
