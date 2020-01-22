package org.biodatageeks

import java.io.File

import com.holdenkarau.spark.testing.RDDComparisons
import htsjdk.samtools.{SAMRecord, SamReaderFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.CustomRDDSAMRecordFunctions._
import org.biodatageeks.alignment.{AlignmentTools, CommandBuilder, Constants, SeqTenderAlignment}
import org.biodatageeks.conf.InternalParams
import org.biodatageeks.shared.IllegalFileExtensionException
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}


class AlignmentTest extends FunSuite
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

  test("should make fasta rdds on 2 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val rdds = SeqTenderAlignment.makeReadRddsFromFA(InputPaths.faReadsPath)

    assert(rdds.getNumPartitions === 2)
  }

  //bowtie's tests
  test("should return number of aligned and unaligned fastq reads by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 7)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should return number of aligned and unaligned fasta reads by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 7)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 14)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 12)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie - freestyle command") {
    val freestyleCommand = new StringBuilder("docker run --rm -i ")
    freestyleCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    freestyleCommand.append(s"${Constants.defaultBowtieImage} ")
    freestyleCommand.append("bowtie -t ") // -t - print time required for the process
    freestyleCommand.append("--threads 2 ") // --threads - number of threads
    freestyleCommand.append("-S ")
    freestyleCommand.append("/data/e_coli_short ")
    freestyleCommand.append(s"--sam-RG ID:${Constants.defaultBowtieRGId} --sam-RG ${Constants.defaultBowtieRG} ")
    freestyleCommand.append("- ")

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, freestyleCommand.toString)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 7)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  // bowtie2's tests
  test("should return number of aligned and unaligned fastq reads by bowtie2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned fasta reads by bowtie2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bowtie2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 20)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should return number of aligned and unaligned fastq reads by bowtie2 - freestyle command") {
    val freestyleCommand = new StringBuilder("docker run --rm -i ")
    freestyleCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    freestyleCommand.append(s"${Constants.defaultBowtie2Image} ")
    freestyleCommand.append("bowtie2 -t ") // -t - print time required for the process
    freestyleCommand.append("--threads 2 ") // --threads - number of threads
    freestyleCommand.append("-x ") // --threads - number of threads
    freestyleCommand.append(s"/data/e_coli_short --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} ")
    freestyleCommand.append("- ")

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, freestyleCommand.toString)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 10)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 3)
  }

  // minimap2's tests
  test("should return number of aligned and unaligned fastq reads by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 3)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 10)
  }

  test("should return number of aligned and unaligned fasta reads by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 3)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 10)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 6)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 20)
  }

  test("should return number of aligned and unaligned fastq reads by minimap2 - freestyle command") {
    val freestyleCommand = new StringBuilder("docker run --rm -i ")
    freestyleCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    freestyleCommand.append(s"${Constants.defaultMinimap2Image} ")
    freestyleCommand.append("minimap2 -a -x map-ont --seed 42 ") // --seed - for randomizing equally best hits
    freestyleCommand.append("-t 2 ") // -t - number of threads
    freestyleCommand.append(s"""-R "@RG\\tID:${Constants.defaultBowtieRGId}\\t${Constants.defaultBowtieRG}" """)
    freestyleCommand.append("/data/e_coli_short.fa ")
    freestyleCommand.append("- ")

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, freestyleCommand.toString)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 3)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 10)
  }

  // bwa's tests
  test("should return number of aligned and unaligned fastq reads by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 9)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned fasta reads by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 9)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  test("should return number of aligned and unaligned interleaved fastq reads by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 20)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 6)
  }

  test("should return number of aligned and unaligned fastq reads by bwa - freestyle command") {
    val freestyleCommand = new StringBuilder("docker run --rm -i ")
    freestyleCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    freestyleCommand.append(s"${Constants.defaultBWAImage} ")
    freestyleCommand.append("bwa mem -t 2 ") // -t - number of threads
    freestyleCommand.append(s"""-R "@RG\\tID:${Constants.defaultBowtieRGId}\\t${Constants.defaultBowtieRG}" """)
    freestyleCommand.append("/data/e_coli_short.fa ")
    freestyleCommand.append("- ")

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, freestyleCommand.toString)
    val collectedSam = sam.collect

    assert(collectedSam.count(it => it.getAlignmentStart !== SAMRecord.NO_ALIGNMENT_START) === 9)
    assert(collectedSam.count(it => it.getAlignmentStart === SAMRecord.NO_ALIGNMENT_START) === 4)
  }

  // exception
  test("should thrown IllegalFileExtensionException when try align reads with invalid extension") {
    val command = "command"

    val thrown = intercept[IllegalFileExtensionException] {
      SeqTenderAlignment.pipeReads(InputPaths.invalidReadsPath, command)
    }

    assert(thrown.getMessage === "Reads file isn't a fasta or fastq file")
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

  // save RDD tests
  test("should save RDD[SAMRecord] to BAM with Hadoop-BAM") {

    val method = "hadoop-bam"
    sparkSession
      .conf
      .set(InternalParams.BAM_IO_LIB, method)

    val outputPath = s"/tmp/test_${method}.bam"
    FileUtils.deleteQuietly(new File(outputPath))
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
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

    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
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
