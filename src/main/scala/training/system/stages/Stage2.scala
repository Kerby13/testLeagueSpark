package training.system.stages

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object Stage2 {

  def load(ss: SparkSession, params: Parameters): DataFrame = {
    import ss.implicits._

    val dimBanInfo = ss.table(params.DIM_BAN_INPUT_TABLE)
      .select(
        'BAN_KEY.alias("ban").cast(String),
        'CUST_FULLNAME.alias("name").cast(String)
      )

    val dimSubscriberInfo = ss.table(params.DIM_SUBSCRIBER_INPUT_TABLE)
      .select(
        'BAN_KEY.alias("ban").cast(String),
        'SUBS_KEY.alias("phone").cast(String)
      ).join(dimBanInfo, 'ban === 'ban)
      .select('phone,'name)
    dimSubscriberInfo
  }
}
