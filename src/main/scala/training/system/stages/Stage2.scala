package training.system.stages

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class Stage2 extends Serializable {
  private val sparkSession: SparkSession = SparkSession.builder.getOrCreate()

  import sparkSession.implicits._

  def load(sc: SparkContext): DataFrame = {
    val sc1 = sc
      .textFile(Paths.path_stage2_ban)
      .map(line => line.split("\u0001", -1))
      .map(array => (array(0), array(14)))
      .map {
        case (ban, name) => Row(ban, name)
      }
    val sc2 = sc
      .textFile(Paths.path_stage3_subs)
      .map(line => line.split("\u0001", -1))
      .map(array => (array(0), array(1)))
      .map {
        case (ban, phone) => Row(ban, phone)
      }

    val banStruct = StructType(
      StructField("ban", StringType, nullable = true) ::
        StructField("name", StringType, nullable = true) :: Nil
    )
    val subsStruct = StructType(
      StructField("ban", StringType, nullable = true) ::
        StructField("phone", StringType, nullable = true) :: Nil
    )

    sparkSession.createDataFrame(sc1, banStruct).createOrReplaceTempView("banView")
    val banView = sparkSession.table("banView")

    sparkSession.createDataFrame(sc2, subsStruct).createOrReplaceTempView("subsView")
    val subsView = sparkSession.table("subsView")
    val resView = subsView.join(banView, "ban").select("phone", "name")
    resView

  }
}

