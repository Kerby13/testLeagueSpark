package training.system.stages

import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object Stage2 {

  def load(ss: SparkSession, params: Parameters): DataFrame = {
    import ss.implicits._

    val dimBanInfo = ss.table(params.DIM_BAN_INPUT_TABLE)
      .select(
        'BAN_KEY.alias("ban"),
        'CUST_FULLNAME.alias("name")
      )

    val dimSubscriberInfo = ss.table(params.DIM_SUBSCRIBER_INPUT_TABLE)
      .select(
        'BAN_KEY.alias("ban"),
        'SUBS_KEY.alias("phone")
      ).join(dimBanInfo, 'ban === 'ban)
      .select('phone,'name)
    dimSubscriberInfo
  }
}
