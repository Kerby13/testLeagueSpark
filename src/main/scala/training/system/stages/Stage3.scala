package training.system.stages

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Stage3 {
  def load(stage_1: DataFrame, stage_2: DataFrame): DataFrame = {
    stage_1.join(stage_2, "phone")
  }
}
