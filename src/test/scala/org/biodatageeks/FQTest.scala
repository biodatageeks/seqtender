package org.biodatageeks

import htsjdk.samtools.SAMRecord
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.{CommandBuilder, Constants, SeqTenderAlignment}
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

// todo: read about PrivateMethodTester
class FQTest extends FunSuite with BeforeAndAfter with PrivateMethodTester {

  val sparkSession: SparkSession = SparkSession
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

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should make fasta rdds on 2 partitions") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val rdds = SeqTenderAlignment.makeReadRddsFromFA(sparkSession, readsDescription.getReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return number of aligned and unaligned fastq reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned fasta reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned interleaved fastq reads") {
    val readsDescription = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val sam = SeqTenderAlignment.pipeReads(readsDescription, sparkSession)

    assert(sam.collect.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 20)
    assert(sam.collect.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }
}
