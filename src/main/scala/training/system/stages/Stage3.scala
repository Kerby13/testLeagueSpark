package training.system.stages

import org.apache.spark.rdd.RDD


class Stage3 {
  def load(rdd1: RDD[(String, (String, Array[Int]))], rdd2: RDD[(String, String)]): RDD[(String, String, String)] = {
    rdd1.leftOuterJoin(rdd2).map {
      case (phone, ((date, arr), name)) =>
        (phone, arr.mkString(","), name.getOrElse("NO_DATA"))
    }
  }
}
