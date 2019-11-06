package training.system.stages

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


class Stage3 {

  def load(df1: DataFrame, df2: DataFrame): DataFrame = {
    val result = df1.join(df2, df1("phone") === df2("phone"), "left_outer").drop(df2("phone"))
    result
  }
}
