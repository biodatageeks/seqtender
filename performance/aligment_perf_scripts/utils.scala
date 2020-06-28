
import org.apache.hadoop.fs.{FileSystem, Path}
import org.biodatageeks.alignment.{SeqTenderAlignment,AlignmentTools}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode


def prepareMetadata(sc: SparkContext): (String, Int, Int, Int, Long) = {
  val sparkMode = if(sc.master.toLowerCase.startsWith("local")) "local" else "cluster"
  val cores = sc.getConf.getInt("spark.executor.instances",sc
    .master
    .toLowerCase
    .replace("local","")
    .replace("[","")
    .replace("]","") match {
    case "*" => -1
    case "yarn" => -1
    case r:String  =>r.toInt
  })* sc.getConf.getInt("spark.executor.cores",1)

  val driverMem = sc.getConf.getSizeAsGb("spark.driver.memory","0").toInt
  val execMem =  sc.getConf.getSizeAsGb("spark.executor.memory","0").toInt

  val defaultSplit=134217728.toLong

  val splitSize = if (sc.getConf.contains("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize"))
    sc.getConf.get("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize").toLong else defaultSplit
  
  (sparkMode, cores, driverMem, execMem, splitSize )
}

def calculatePartitionsNum (readsPath: String): Int = {

  val readsExtension = AlignmentTools.getReadsExtension(readsPath)
  val rdd = SeqTenderAlignment.makeReadRDD(readsPath)(spark) 

  rdd.getNumPartitions
}

def calculateFileSize(path:String): Long ={
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  fs.getContentSummary(new Path(path)).getLength
}

def saveResults(id:String, piper:String, tool:String, format:String,timeTaken:Long, sparkMode:String, cores:Int, driverMem: Int, execMem:Int, partitionsNum:Long, rowCount:Long, splitSize:Long, fileSize:Long) ={
  import spark.implicits._
  val res = Seq((id, piper, tool, format, timeTaken, sparkMode, cores, driverMem, execMem, partitionsNum, rowCount, splitSize,fileSize))
    .toDF("test_id", "piper", "tool", "format", "time_taken", "spark_mode", "cores", "driver_mem", "exec_mem", "partitions_num", "row_count", "split_size", "file_size")
  val opts = Map(
    "driver" -> "org.postgresql.Driver",
    "dbtable" -> "al_perf",
    "url" -> sys.env("BDG_PERF_DB"),
    "user" -> sys.env("BDG_PERF_USER"),
    "password" -> sys.env("BDG_PERF_PASS")
  )
  res.write.format("jdbc").options(opts).mode(SaveMode.Append).save
}