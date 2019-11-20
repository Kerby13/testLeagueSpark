package training.system

import org.apache.hadoop.fs.FileSystem

object Parameters extends Serializable {
  def instance(args: Array[String], fs: FileSystem) = new Parameters(args, fs)
}

class Parameters(args: Array[String], fs: FileSystem) extends Serializable {
  val EMPTY_PATH = "/user/..."
  private val paramMap = args
    .map(param => {
      val pair = param.split("=", -1)
      if (pair.length == 3)
        (pair(0), pair(1) + "=" + pair(2))
      else
        (pair(0), pair(1))
    }).toMap

  val CDR_INPUT_TABLE = paramMap.getOrElse("CMD_INPUT_PATH", EMPTY_PATH)
  val DIM_BAN_INPUT_TABLE = paramMap.getOrElse("DIM_BAN_INPUT_PATH", EMPTY_PATH)
  val DIM_SUBSCRIBER_INPUT_TABLE = paramMap.getOrElse("DIM_SUBSCRIBER_INPUT_PATH", EMPTY_PATH)
  val RES_OUTPUT_PATH = paramMap.getOrElse("RES_OUTPUT_PATH", EMPTY_PATH)
  val DATE_PART = paramMap.getOrElse("DATE_PART", EMPTY_PATH)

  //val REPORT_DATE_YYYY_MM_DD = paramMap.getOrElse("REPORT_DATE_YYYY_MM_DD", EMPTY_PATH)

  //if (fs.exists(new Path(RES_OUTPUT_PATH))) {
  //  fs.delete(new Path(RES_OUTPUT_PATH + "/time_key=" + REPORT_DATE_YYYY_MM_DD), true)
  //}
}