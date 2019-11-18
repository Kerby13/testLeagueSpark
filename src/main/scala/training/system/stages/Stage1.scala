package training.system.stages


import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters
import org.apache.spark.sql.functions.{when, _}
//import import org.apache.spark.sql.functions._ обернуть в lit если нужно, case when *проверка времени* 1,
//                                                                                  else 0 и название столбца

object Stage1 {
  def load(ss: SparkSession, params: Parameters): DataFrame = {
    import ss.implicits._
    val cdrInfo = ss.table(params.CDR_INPUT_TABLE)
      .select(
        'subscriber_no.alias("phone").cast(String),
        'channel_seizure_date_time.alias("date_time").cast(String),
        'basic_service_type.alias("type").cast(String))
      .withColumn("call_morning",
        when(substring(col("date_time"), 8, 2) >= "6" &&
          substring(col("date_time"), 8, 2) < "11" &&
          col("type") === "V", 1)
          .otherwise(0))
      .withColumn("call_day",
        when(substring(col("date_time"), 8, 2) >= "11" &&
          substring(col("date_time"), 8, 2) < "19" &&
          col("type") === "V", 1)
          .otherwise(0))
      .withColumn("call_evening",
        when(substring(col("date_time"), 8, 2) >= "19" &&
          substring(col("date_time"), 8, 2) < "23" &&
          col("type") === "V", 1)
          .otherwise(0))
      .withColumn("call_night",
        when(substring(col("date_time"), 8, 2) >= "23" ||
          substring(col("date_time"), 8, 2) < "6" &&
            col("type") === "V", 1)
          .otherwise(0))
      .withColumn("sms_morning",
        when(substring(col("date_time"), 8, 2) >= "6" &&
          substring(col("date_time"), 8, 2) < "11" &&
          col("type") === "S", 1)
          .otherwise(0))
      .withColumn("sms_day",
        when(substring(col("date_time"), 8, 2) >= "11" &&
          substring(col("date_time"), 8, 2) < "19" &&
          col("type") === "S", 1)
          .otherwise(0))
      .withColumn("sms_evening",
        when(substring(col("date_time"), 8, 2) >= "19" &&
          substring(col("date_time"), 8, 2) < "23" &&
          col("type") === "S", 1)
          .otherwise(0))
      .withColumn("sms_night",
        when((substring(col("date_time"), 8, 2) >= "23" ||
          substring(col("date_time"), 8, 2) < "6") &&
          col("type") === "S", 1)
          .otherwise(0))
    cdrInfo
  }
}
