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

    val rdds = SeqTenderAlignment.makeReadRddsFromFQ(InputPaths.fqReadsPath)

    assert(rdds.getNumPartitions === 3)
  }

  test("should make interleaved fastq rdds on 5 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)

    val rdds = SeqTenderAlignment.makeReadRddsFromIFQ(InputPaths.ifqReadsPath)

    assert(rdds.getNumPartitions === 5)
  }

  test("should make fasta rdds on 2 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val rdds = SeqTenderAlignment.makeReadRddsFromFA(InputPaths.faReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  // fasta
  test("should thrown SparkException contains SPLIT FASTA exception message when try align reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceFaReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastaReadInputFormat.isFastaReadRead()
    assert(thrown.getMessage.contains("[SPLIT FASTA]: Unexpected character in sequence in fasta record"))
  }

  test("should thrown SparkException contains SPLIT FASTA exception message when try align reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidNameFaReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastaReadInputFormat.isFastaReadRead()
    assert(thrown.getMessage.contains("[SPLIT FASTA]: Unexpected character in name in fasta record"))
  }

  // fastq
  test("should thrown SparkException contains SPLIT FASTQ exception message when try align reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceFqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT FASTQ]: Unexpected character in sequence in fastq record"))
  }

  test("should thrown SparkException contains SPLIT FASTQ exception message when try align reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidNameFqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT FASTQ]: Unexpected character in name in fastq record"))
  }

  test("should thrown SparkException contains SPLIT FASTQ exception message when try align reads with invalid separator") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidSeparatorFqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("This should be '+' separator."))
  }

  test("should thrown SparkException contains SPLIT FASTQ exception message when try align reads with invalid quality") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidQualityFqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from FastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT FASTQ]: Unexpected character in quality in fastq record"))
  }

  // ifq
  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with invalid sequence") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidSequenceIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT INTERLEAVED FASTQ]: Unexpected character in sequence in interleaved fastq record"))
  }

  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with invalid name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidNameIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT INTERLEAVED FASTQ]: Unexpected character in name in interleaved fastq record"))
  }

  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with invalid separator") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidSeparatorIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("This should be '+' separator."))
  }

  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with invalid quality") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidQualityIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT INTERLEAVED FASTQ]: Unexpected character in quality in interleaved fastq record"))
  }

  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with invalid first read name") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.invalidFirstReadNameIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT INTERLEAVED FASTQ]: Read name in interleaved fastq record isn't correct. It hasn't proper ordinal number."))
  }

  test("should thrown SparkException contains SPLIT INTERLEAVED FASTQ exception message when try align reads with no second read") {
    val command = "command"

    val thrown = intercept[SparkException] {
      val collected = SeqTenderAlignment.pipeReads(InputPaths.noSecondReadIfqReadsPath, command).collect
      collected.count(it => it.getReadName != "")
    }

    // unfortunately thrown exception is SparkException,
    // so we have to check if this exception message contains message
    // thrown by RuntimeException from InterleavedFastqReadInputFormat.isFastqReadRead()
    assert(thrown.getMessage.contains("[SPLIT INTERLEAVED FASTQ]: Read name in interleaved fastq record isn't correct. It hasn't proper ordinal number."))
  }
}
