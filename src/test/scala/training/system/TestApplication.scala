package scala.training.system

import org.apache.hadoop.fs.FileSystem
import org.apache.spark._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import training.system.stages.{Stage1, Stage2, Stage3}

class TestApplication extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = _
  implicit var sc: SparkContext = _
  private val resourcePath: String = "src/test/resources/"

  override protected def beforeAll(): Unit = {
    sparkConf = new SparkConf().setAppName("custom_calls_info").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  test("all_stages_test") {
    val stage1 = new Stage1
    val stage2 = new Stage2
    val stage3 = new Stage3

    val res1 = stage1.load(sc)
    val res2 = stage2.load(sc)
    val res3 = stage3.load(res1,res2)
    res3.coalesce(1).saveAsTextFile(resourcePath + "res")
  }
}

