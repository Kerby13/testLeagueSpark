package training.system.stages

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

class Stage1 extends Serializable {
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


  def load(sc: SparkContext): RDD[(String, (String, Array[Int]))] = sc
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
  }
    .reduceByKey {
      case ((v_morn_ac, v_day_ac, v_even_ac, v_night_ac, s_morn_ac, s_day_ac, s_even_ac, s_night_ac), (v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night))
      => (v_morn_ac + v_morn, v_day_ac + v_day, v_even_ac + v_even, v_night_ac + v_night, s_morn_ac + s_morn, s_day_ac + s_day, s_even_ac + s_even, s_night_ac + s_night)
    }.map {
    case ((phone, date), (v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night)) =>
      (phone, (date, Array(v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night)))
  }


}
