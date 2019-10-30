package training.system.stages

import org.apache.spark.rdd.RDD


class Stage3 {
  def load(rdd1: RDD[(String, Array[Any])], rdd2: RDD[(String, String)]): RDD[(String, Array[Any])] = {
    rdd1.leftOuterJoin(rdd2).map {
      case (phone, (Array(date, v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night), name)) =>
        (phone, Array(date, v_morn, v_day, v_even, v_night, s_morn, s_day, s_even, s_night, name.getOrElse("NO_DATA")))
    }
  }
}
