package training.system

import java.io.File

import org.apache.hadoop.fs.FileSystem
import com.google.common.base.Joiner
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import training.system.stages.{Stage1, Stage2, Stage3}

class TestApplication extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = _
  implicit var sc: SparkContext = _
  private val resourcePath: String = "src/test/resources/1"
  private val columnNames = Seq("Phone_number", "Calls&sms activity", "Name")


  override protected def beforeAll(): Unit = {
    sparkConf = new SparkConf().setAppName("custom_calls_info").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  test("all_stages_test") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val stage1 = new Stage1
    val stage2 = new Stage2
    val stage3 = new Stage3

    val st1 = stage1.load(sc)
    val st2 = stage2.load(sc)
    val result = stage3.load(st1, st2)
    result.show(false)
  }
}

