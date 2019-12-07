package org.biodatageeks

import htsjdk.samtools.SAMRecord
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.{CommandBuilder, Constants, SeqTenderAlignment}
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

// todo: read about PrivateMethodTester
class FQTest extends FunSuite with BeforeAndAfter with PrivateMethodTester {
  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
  }

  after {
    sparkSession.close()
  }

  // bowtie's tests
  test("should make fastq rdds on 2 partitions by bowtie") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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
      tool = Constants.bowtie2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 11)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned fasta reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 11)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bowtie2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
    val collectedSam = sam.collect

    println("bowtie2")
    collectedSam.foreach(it => println(it.getAlignmentStart))

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

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by minimap2") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make fasta rdds on 2 partitions by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads by bwa") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
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

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 22)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }
}
