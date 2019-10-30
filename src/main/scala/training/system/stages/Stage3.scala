package training.system.stages

import org.apache.spark.rdd.RDD


class Stage3 {
  def load(rdd1: RDD[(String, (String, Array[Int]))], rdd2: RDD[(String, String)]): RDD[(String, Int, Int, Int, Int, Int, Int, Int, Int, String)] = {
    rdd1.leftOuterJoin(rdd2).map {
      case (phone, ((date, Array(x1, x2, x3, x4, x5, x6, x7, x8)), name)) =>
        (phone, x1, x2, x3, x4, x5, x6, x7, x8, name.getOrElse("NO_DATA"))
    }
  }
}
