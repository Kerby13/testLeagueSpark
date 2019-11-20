package training.system

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import training.system.stages.{Stage1, Stage2, Stage3}

object Main {
  def main(args: Array[String]): Unit = {
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val parameters = Parameters.instance(args, fs)
//    val spark = SparkSession
//      .builder
//      .appName("testLeagueSpark")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.conf.set("spark.sql.caseSensitive", "false")
//    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
//    spark.conf.set("hive.exec.dynamic.partition", "true")
//    spark.conf.set("spark.sql.hive.convertMetastoreOrc", "false")

    val ss = SparkSession.builder().appName("cdr_dim_ban_test").enableHiveSupport().getOrCreate()

    if (fs.exists(new Path(parameters.RES_OUTPUT_PATH))) {
      fs.delete(new Path(parameters.RES_OUTPUT_PATH), true)
    }

    val stage1_res = Stage1.load(ss,parameters)
    val stage2_res = Stage2.load(ss,parameters)
    val stage3_res = Stage3.load(stage1_res, stage2_res)
    stage3_res.write.format("csv").save(parameters.RES_OUTPUT_PATH)
    ss.stop()
    //  spark.stop()
  }
}