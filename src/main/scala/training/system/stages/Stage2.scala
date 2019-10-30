package training.system.stages

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Stage2 extends Serializable {

  def load(sc: SparkContext): RDD[(String, String)] = {
    val sc1 = sc
      .textFile(Paths.path_stage2_ban)
      .map(line => line.split("\u0001", -1))
      .map(array => (array(0), array(14)))
    val sc2 = sc
      .textFile(Paths.path_stage3_subs)
      .map(line => line.split("\u0001", -1))
      .map(array => (array(0), array(1)))

    sc2.leftOuterJoin(sc1).map{
      case (ban, (phone, name)) => (phone, name.getOrElse("NO_DATA"))
    }
  }

}
