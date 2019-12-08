package org.biodatageeks

import java.io.File

import com.holdenkarau.spark.testing.RDDComparisons
import htsjdk.samtools.{SAMRecord, SamReaderFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.{CommandBuilder, Constants, SeqTenderAlignment}
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}
import org.biodatageeks.CustomRDDSAMRecordFunctions._


// todo: read about PrivateMethodTester
class FQTest extends FunSuite
  with BeforeAndAfter
  with PrivateMethodTester
  with RDDComparisons{

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  before {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
  }

  test("should make fastq rdds on 2 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should make fasta rdds on 2 partitions") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned fasta reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned interleaved fastq reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 20)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should save RDD[SAMRecord] to BAM") {


    val outputPath = "/tmp/test.bam"
    FileUtils.deleteQuietly(new File(outputPath))
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    sam.saveAsBAMFile(outputPath)

    val samReader = SamReaderFactory.makeDefault().open(new File(outputPath))
    import collection.JavaConverters._
    val readRecords = samReader.iterator().asScala.toArray

    //equal count
    assert(sam.count() === readRecords.length)

    //RNEXT problem (7th column diff "=" <> "*") in case of unmapped reads - hadoop-bam
//    r3/1    4       *       0       0       *       =       0       0       CGATGCAGATGCGTACCACCTGGACCAGGCCTTTC     EDCCCBAAAA@@@@?>===<;;9:99987776554     RG:Z:dummy      YT:Z:UU
//
//    r3/1    4       *       0       0       *       *       0       0       CGATGCAGATGCGTACCACCTGGACCAGGCCTTTC     EDCCCBAAAA@@@@?>===<;;9:99987776554     RG:Z:dummy      YT:Z:UU

    assertRDDEquals(sam.filter(_.getReferenceName != "*"), sparkSession.sparkContext.parallelize(readRecords.filter(_.getReferenceName != "*") ) )


  }
}
