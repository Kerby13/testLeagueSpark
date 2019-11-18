package training.system

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import training.system.stages
import training.system.stages.{Stage1, Stage2, Stage3}

object Main {
  def main(args: Array[String]): Unit = {
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val parameters = Parameters.instance(args, fs)
    val spark = SparkSession
      .builder
      .appName("testLeagueSpark")
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreOrc", "false")

    val sparkConf = new SparkConf().setAppName("cdr_dim_ban_test").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val ss = SparkSession.builder().appName("cdr_dim_ban_test").enableHiveSupport().getOrCreate()

    val stage1_res = Stage1.load(ss,parameters)


    //    val stage1 = new Stage1
    //    val stage2 = new Stage2
    //    val stage3 = new Stage3
    //
    //    val stage1_res = stage1.load(ss, parameters)
    //    val stage2_res = stage2.load(sc, parameters)
    //    val stage3_res = stage3.load(stage1_res, stage2_res)
    //    stage3_res.saveAsTextFile(parameters.RES_OUTPUT_PATH)

      spark.stop()
  }
}