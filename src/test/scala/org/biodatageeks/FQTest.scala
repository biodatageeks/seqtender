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
import org.biodatageeks.conf.InternalParams


// todo: read about PrivateMethodTester
class FQTest extends FunSuite
  with BeforeAndAfter
  with PrivateMethodTester
  with RDDComparisons {

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  before {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
  }

  after {
    sparkSession.sparkContext.hadoopConfiguration.clear()
  }

  // bowtie's tests
  test("should make fastq rdds on 2 partitions by bowtie") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned fasta reads by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 20)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 8)
  }

  // bowtie2's tests
  test("should make fastq rdds on 2 partitions by bowtie2") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 11)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned fasta reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 11)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 22)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  // minimap2's tests
  test("should make fastq rdds on 2 partitions by minimap2") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 1)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 13)
  }

  test("should return number of aligned and unaligned fasta reads by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 1)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 13)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 2)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 26)
  }

  // bwa's tests
  test("should make fastq rdds on 2 partitions by bwa") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned fasta reads by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 22)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should save RDD[SAMRecord] to BAM with Hadoop-BAM") {

    val method = "hadoop-bam"
    sparkSession
      .conf
      .set(InternalParams.BAM_IO_LIB, method)

    val outputPath = s"/tmp/test_${method}.bam"
    FileUtils.deleteQuietly(new File(outputPath))
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    sam.saveAsBAMFile(outputPath)

    val samReader = SamReaderFactory.makeDefault().open(new File(outputPath))
    import collection.JavaConverters._
    val readRecords = samReader.iterator().asScala.toArray

    //equal count
    assert(sam.count() === readRecords.length)

    //Prior adding disq dependency there was a problem with unmapped reads for hadoop-bam
    //RNEXT problem (7th column diff "=" <> "*") in case of unmapped reads - hadoop-bam
    //    r3/1    4       *       0       0       *       =       0       0       CGATGCAGATGCGTACCACCTGGACCAGGCCTTTC     EDCCCBAAAA@@@@?>===<;;9:99987776554     RG:Z:dummy      YT:Z:UU
    //
    //    r3/1    4       *       0       0       *       *       0       0       CGATGCAGATGCGTACCACCTGGACCAGGCCTTTC     EDCCCBAAAA@@@@?>===<;;9:99987776554     RG:Z:dummy      YT:Z:UU


    assertRDDEquals(sam, sparkSession.sparkContext.parallelize(readRecords))
  }

  test("should save RDD[SAMRecord] to BAM with disq") {

    val method = "disq"
    sparkSession
      .conf
      .set(InternalParams.BAM_IO_LIB, method)
    val outputPath = s"/tmp/test_${method}.bam"
    FileUtils.deleteQuietly(new File(outputPath))
    //remove indexes
    val sbiIndex = new File(s"${outputPath}.sbi")
    val baiIndex = new File(s"${outputPath}.bai")
    FileUtils.deleteQuietly(baiIndex)
    FileUtils.deleteQuietly(sbiIndex)

    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription)
    sam.saveAsBAMFile(outputPath)

    val samReader = SamReaderFactory.makeDefault().open(new File(outputPath))
    import collection.JavaConverters._
    val readRecords = samReader.iterator().asScala.toArray

    //equal count
    assert(sam.count() === readRecords.length)
    //equal contents
    assertRDDEquals(sam, sparkSession.sparkContext.parallelize(readRecords))

    // should have RG
    assert(!samReader.getFileHeader.getReadGroups.isEmpty)

    //assert indexes
    assert(sbiIndex.exists())
    assert(baiIndex.exists())


  }
}
