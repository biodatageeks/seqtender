import org.biodatageeks.alignment.{SeqTenderAlignment,AlignmentTools,ReadsExtension,CommandBuilder, Constants}
import org.biodatageeks.alignment.{SeqTenderAlignment,AlignmentTools,ReadsExtension,CommandBuilder, Constants}
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.cannoli.Cannoli._
import org.bdgenomics.cannoli.BwaArgs
import org.bdgenomics.cannoli.Bowtie2Args
import org.bdgenomics.cannoli.SingleEndBowtie2Args



val readsPathList= if (testMode == "FULL") Array("/user/pkarbownik/cannoli_data/reads/SRR622461_2.fastq")  else
      Array("/user/pkarbownik/cannoli_data/reads/corriell_S7_L001_R1_001.fq",
        "/user/pkarbownik/cannoli_data/reads/corriell_S7_L001_interleaved.ifq")


var testId = ""
var partitionsNum, fileSize:Long = 0
val (sparkMode, cores, driverMem, execMem, splitSize) = prepareMetadata(sc)


def alignWithSeqtender(readsPath:String, indexPath:String, toolName:String): (String, Long, Long) = {
  val format = AlignmentTools.getReadsExtension(readsPath)
  val alignCmd = CommandBuilder.buildCommand(
    readsExtension = format,
    indexPath = indexPath,
    tool= toolName,
    image = null,
    interleaved = if (format.equals(ReadsExtension.IFQ)) true else false,
    readGroup = Constants.defaultRG,
    readGroupId = Constants.defaultRGId
    )
    val time1 = System.currentTimeMillis()
    val rowCount = SeqTenderAlignment.pipeReads(readsPath, alignCmd)(spark).count().toLong
    val time2 = System.currentTimeMillis()
    val timeTaken = time2-time1
    (format.toString, rowCount, timeTaken)
}

def alignWithCannoli(readsPath:String, indexPath:String, toolName:String): (String, Long, Long) = {
  val format = AlignmentTools.getReadsExtension(readsPath)
  val sampleId = "corriell"
  if (format.equals(ReadsExtension.IFQ)) {
    toolName match {
      case Constants.bwaToolName => { 
        val args = new BwaArgs()
        args.indexPath = indexPath
        args.sampleId = sampleId
        args.useDocker = true
        val time1 = System.currentTimeMillis()
        val reads = sc.loadInterleavedFastqAsFragments(readsPath)
        val rowCount = reads.alignWithBwa(args).rdd.count().toLong
        val time2 = System.currentTimeMillis()
        val timeTaken = time2-time1
        (format.toString, rowCount,timeTaken)
      }
      case Constants.bowtie2ToolName => {
        val args = new Bowtie2Args()
        args.indexPath = indexPath
        args.useDocker = true
        val reads = sc.loadInterleavedFastqAsFragments(readsPath)
        val time1 = System.currentTimeMillis()
        val rowCount = reads.alignWithBowtie2(args).rdd.count()
        val time2 = System.currentTimeMillis()
        val timeTaken = time2-time1
        (format.toString,rowCount,timeTaken)
      }
    }
  } else if (format.equals(ReadsExtension.FQ)) {
    toolName match {
      case Constants.bwaToolName => throw new RuntimeException(s"$toolName for ${ReadsExtension.FQ} not supported in $piper")
      case Constants.bowtie2ToolName => {
        val args = new SingleEndBowtie2Args()
        args.indexPath = indexPath
        args.useDocker = true
        val reads = sc.loadUnpairedFastq(readsPath)
        val time1 = System.currentTimeMillis()
        val rowCount = reads.alignWithBowtie2(args).rdd.count()
        val time2 = System.currentTimeMillis()
        val timeTaken = time2-time1
        (format.toString,rowCount,timeTaken)
      }
    }
  } else
    throw new RuntimeException("unknown format");
}

for (readsPath <- readsPathList) {
  testId = toolName + "_" + System.currentTimeMillis()
  fileSize = calculateFileSize(readsPath)
  partitionsNum = calculatePartitionsNum(readsPath)

  var (format, rowCount,timeTaken) = piper match {
    case "SEQTENDER" => alignWithSeqtender(readsPath, indexPath, toolName)
    case "CANNOLI" => alignWithCannoli(readsPath, indexPath, toolName)
  } 

  saveResults(testId,piper,toolName, format.toString, timeTaken,sparkMode,cores,driverMem,execMem, partitionsNum, rowCount,splitSize,fileSize)
}

System.exit(0)
