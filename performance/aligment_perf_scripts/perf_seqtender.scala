
import org.biodatageeks.alignment.{SeqTenderAlignment,AlignmentTools,ReadsExtension}
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

  val splitSize = sc.getConf.get("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize").toLong

  (sparkMode, cores, driverMem, execMem, splitSize )
}

def saveResults(id:String, piper:String, tool:String, format:String,timeTaken:Long, sparkMode:String, cores:Int, driverMem: Int, execMem:Int, partitionsNum:Long, rowCount:Long, splitSize:Long) ={
  import spark.implicits._
  val res = Seq((id, piper, tool, format, timeTaken, sparkMode, cores, driverMem, execMem, partitionsNum, rowCount, splitSize))
    .toDF("test_id", "piper", "tool", "format", "time_taken", "spark_mode", "cores", "driver_mem", "exec_mem", "partitions_num", "row_count", "split_size")
  val opts = Map(
    "driver" -> "org.postgresql.Driver",
    "dbtable" -> "al_perf",
    "url" -> sys.env("BDG_PERF_DB"),
    "user" -> sys.env("BDG_PERF_USER"),
    "password" -> sys.env("BDG_PERF_PASS")
  )
  res.write.format("jdbc").options(opts).mode(SaveMode.Append).save
}

def calculatePartitionsNum (readsPath: String): Int = {

  val readsExtension = AlignmentTools.getReadsExtension(readsPath)

  val rdd = SeqTenderAlignment.makeReadRDD(readsPath)(spark) 

  rdd.getNumPartitions
}

sc.setLogLevel("ERROR")


val (sparkMode, cores, driverMem, execMem, splitSize) = prepareMetadata(sc)
var piper = "seqtender"

var toolName, testId, readsPath, indexPath, dockerImg, alignCmd, format:String = ""
var time1, time2, timeTaken, partitionsNum, rowCount:Long =0

// BWA test
toolName="bwa"
testId = toolName + "_" + System.currentTimeMillis()

readsPath= "/user/pkarbownik/cannoli_data/reads/corriell_S7_L001_interleaved.ifq"
format = readsPath.split('.').last
indexPath= "/data/samples/NA12878/cannoli_calculate/Sequence/BWAIndex"
dockerImg = "quay.io/biocontainers/bwa:0.7.17--ha92aebf_3"
alignCmd = s"""docker run --rm -i
    -v $indexPath:/data
    $dockerImg bwa mem
    -R "@RG\\tID:test_id\\tSM:Pool1"
    -p /data/genome.fa -
""".replace("\n   ", "")

partitionsNum = calculatePartitionsNum(readsPath)

time1 = System.currentTimeMillis()

rowCount = SeqTenderAlignment.pipeReads(readsPath, alignCmd)(spark).count()
time2 = System.currentTimeMillis()
timeTaken = time2-time1

saveResults(testId,piper,toolName, format, timeTaken,sparkMode,cores,driverMem,execMem, partitionsNum, rowCount,splitSize)

//bowtie 2
toolName="bowtie2"
testId = toolName + "_" + System.currentTimeMillis()

readsPath= "/user/pkarbownik/cannoli_data/reads/corriell_S7_L001_interleaved.ifq"
format = readsPath.split('.').last
indexPath= "/data/samples/NA12878/cannoli_calculate/Sequence/Bowtie2Index"
dockerImg = "quay.io/biocontainers/bowtie2:2.4.1--py38he513fc3_0 "
alignCmd = s"""docker run --rm -i
    -v $indexPath:/data
    $dockerImg bowtie2
    --rg-id "test_id"
    --rg "SM:Pool1"
    -x /data/genome
    --interleaved
    -
""".replace("\n   ", "")

partitionsNum = calculatePartitionsNum(readsPath)

time1 = System.currentTimeMillis()
rowCount = SeqTenderAlignment.pipeReads(readsPath, alignCmd)(spark).count()
time2 = System.currentTimeMillis()
timeTaken = time2-time1

saveResults(testId,piper,toolName, format, timeTaken,sparkMode,cores,driverMem,execMem, partitionsNum, rowCount,splitSize)


System.exit(0)