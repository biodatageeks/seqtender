package org.biodatageeks

import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.SeqTenderAlignment
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

class AlignmentPartitionersTest extends FunSuite
  with BeforeAndAfter
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

  // split files tests
  test("should make fastq rdds on 3 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)

    val rdds = SeqTenderAlignment.makeHadoopRDDFromFQ(InputPaths.fqReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make interleaved fastq rdds on 5 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)

    val rdds = SeqTenderAlignment.makeHadoopRDDFromIFQ(InputPaths.ifqReadsPath)

    assert(rdds.getNumPartitions === 5)
  }

  test("should make fasta rdds on 2 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val rdds = SeqTenderAlignment.makeHadoopRDDFromFA(InputPaths.faReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  // fasta
  test("should thrown SparkException contains READER exception message when try align fasta reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceFaReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastaReadInputFormat.isFastaReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's sequence"))
  }

  test("should thrown SparkException contains READER exception message when try align fasta reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidNameFaReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastaReadInputFormat.isFastaReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's name"))
  }

  // fastq
  test("should thrown SparkException contains READER exception message when try align fastq reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceFqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's sequence"))
  }

  test("should thrown SparkException contains READER exception message when try align fastq reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidNameFqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's name"))
  }

  test("should thrown SparkException contains READER exception message when try align fastq reads with invalid separator") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidSeparatorFqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("This should be '+' separator."))
  }

  test("should thrown SparkException contains READER exception message when try align fastq reads with invalid quality") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidQualityFqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's quality"))
  }

  // ifq
  test("should thrown SparkException contains READER exception message when try align interleaved fastq reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceIfqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's sequence"))
  }

  test("should thrown SparkException contains READER exception message when try align interleaved fastq reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidNameIfqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's name"))
  }

  test("should thrown SparkException contains READER exception message when try align interleaved fastq reads with invalid separator") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidSeparatorIfqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("This should be '+' separator."))
  }

  test("should thrown SparkException contains READER exception message when try align interleaved fastq reads with invalid quality") {
    val command = "command"

    val thrown = intercept[SparkException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidQualityIfqReadsPath, command).collect
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[READER]: Unexpected character in read's quality"))
  }
}
