package org.biodatageeks

import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.{AlignmentTools, CommandBuilder, Constants, SeqTenderAlignment}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

class AlignmentQualityTest extends FunSuite
  with BeforeAndAfter {

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  before {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 500)
  }

  after {
    sparkSession.sparkContext.hadoopConfiguration.clear()
  }

  // bowtie - quality tests
  test("should returns correct fastq alignments' details by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "45567778999:9;;<===>?@@@@AAAABCCCDE")

    val r779 = collectedSam("r779")
    assert(r779.getContig === "e_coli:15000-66000")
    assert(r779.getAlignmentStart === 31224)
    assert(r779.getCigarString === "35M")
    assert(r779.getReadString === "CGAAAAGCTGGGGATGGCAAAACGCGTTAAACCGA")
    assert(r779.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776554")

    val r1000 = collectedSam("r1000")
    assert(r1000.getContig === "e_coli:15000-66000")
    assert(r1000.getAlignmentStart === 484)
    assert(r1000.getCigarString === "75M")
    assert(r1000.getReadString === "GCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTGAACCTCACAGAGGTCTTTTCTCGTTACCAGCGCC")
    assert(r1000.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")
  }

  test("should returns correct fasta alignments' details by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")

    val r779 = collectedSam("r779")
    assert(r779.getContig === "e_coli:15000-66000")
    assert(r779.getAlignmentStart === 31224)
    assert(r779.getCigarString === "35M")
    assert(r779.getReadString === "CGAAAAGCTGGGGATGGCAAAACGCGTTAAACCGA")
    assert(r779.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")

    val r1000 = collectedSam("r1000")
    assert(r1000.getContig === "e_coli:15000-66000")
    assert(r1000.getAlignmentStart === 484)
    assert(r1000.getCigarString === "75M")
    assert(r1000.getReadString === "GCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTGAACCTCACAGAGGTCTTTTCTCGTTACCAGCGCC")
    assert(r1000.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
  }

  // bowtie
  test("should returns correct ifastq alignments' details by bowtie") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.map(record => (
      (// key
        record.getReadName,
        (record.getFlags & Constants.FIRST_SEGMENT_FLAG) == Constants.FIRST_SEGMENT_FLAG,
        (record.getFlags & Constants.LAST_SEGMENT_FLAG) == Constants.LAST_SEGMENT_FLAG
      ), record)
    ).collectAsMap()

    val r14First = collectedSam(("r14", true, false))
    assert(r14First.getFlags === 99)
    assert(r14First.getContig === "e_coli:15000-66000")
    assert(r14First.getAlignmentStart === 24900)
    assert(r14First.getCigarString === "35M")
    assert(r14First.getReadString === "GTAATTTGAGTAATGCCCACCAGTTCCATCACGAT")
    assert(r14First.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776554")

    val r14Last = collectedSam(("r14", false, true))
    assert(r14Last.getFlags === 147)
    assert(r14Last.getContig === "e_coli:15000-66000")
    assert(r14Last.getAlignmentStart === 25105)
    assert(r14Last.getCigarString === "32M")
    assert(r14Last.getReadString === "CACGCACTTTATGCAGTGCTGCCAGCGCCGCC")
    assert(r14Last.getBaseQualityString === "67778999:9;;<===>?@@@@AAAABCCCDE")


    val r294First = collectedSam(("r294", true, false))
    assert(r294First.getFlags === 83)
    assert(r294First.getContig === "e_coli:15000-66000")
    assert(r294First.getAlignmentStart === 32968)
    assert(r294First.getCigarString === "35M")
    assert(r294First.getReadString === "GTAAAAGAGTGCGAAGAGATGATGATCAAACTGTT")
    assert(r294First.getBaseQualityString === "45567778999:9;;<===>?@@@@AAAABCCCDE")

    val r2941Last = collectedSam(("r294", false, true))
    assert(r2941Last.getFlags === 163)
    assert(r2941Last.getContig === "e_coli:15000-66000")
    assert(r2941Last.getAlignmentStart === 32795)
    assert(r2941Last.getCigarString === "32M")
    assert(r2941Last.getReadString === "CCGCGATGTGGTATGTCGGCGCGACCTGCGCC")
    assert(r2941Last.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776")


    val r326First = collectedSam(("r326", true, false))
    assert(r326First.getFlags === 99)
    assert(r326First.getContig === "e_coli:15000-66000")
    assert(r326First.getAlignmentStart === 50520)
    assert(r326First.getCigarString === "35M")
    assert(r326First.getReadString === "GGCTTATCTTCGGCGAAACGGCGGTCAATTTCGTC")
    assert(r326First.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776554")

    val r326Last = collectedSam(("r326", false, true))
    assert(r326Last.getFlags === 147)
    assert(r326Last.getContig === "e_coli:15000-66000")
    assert(r326Last.getAlignmentStart === 50693)
    assert(r326Last.getCigarString === "32M")
    assert(r326Last.getReadString === "CTGAGCAAAAAAGACGTCGTTTTTAAAGCCGT")
    assert(r326Last.getBaseQualityString === "67778999:9;;<===>?@@@@AAAABCCCDE")
  }

  // bowtie2
  test("should returns correct fastq alignments' details by bowtie2") { // todo: ask: should I check all of reads?
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "45567778999:9;;<===>?@@@@AAAABCCCDE")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "42M1I33M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGATCGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9887759853EECBB@@@@?>===<;;9:99987776554")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "26M1D34M1D13M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECB@@@@?>===<;9:99987776554")
  }

  test("should returns correct fasta alignments' details by bowtie2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "50M1I25M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGACGACCTGATCTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "41M1D33M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
  }

  test("should returns correct ifastq alignments' details by bowtie2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.map(record => (
      (// key
        record.getReadName,
        (record.getFlags & Constants.FIRST_SEGMENT_FLAG) == Constants.FIRST_SEGMENT_FLAG,
        (record.getFlags & Constants.LAST_SEGMENT_FLAG) == Constants.LAST_SEGMENT_FLAG
      ), record)
    ).collectAsMap()

    val r14First = collectedSam(("r14", true, false))
    assert(r14First.getFlags === 99)
    assert(r14First.getContig === "e_coli:15000-66000")
    assert(r14First.getAlignmentStart === 24900)
    assert(r14First.getCigarString === "35M")
    assert(r14First.getReadString === "GTAATTTGAGTAATGCCCACCAGTTCCATCACGAT")
    assert(r14First.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776554")

    val r14Last = collectedSam(("r14", false, true))
    assert(r14Last.getFlags === 147)
    assert(r14Last.getContig === "e_coli:15000-66000")
    assert(r14Last.getAlignmentStart === 25105)
    assert(r14Last.getCigarString === "32M")
    assert(r14Last.getReadString === "CACGCACTTTATGCAGTGCTGCCAGCGCCGCC")
    assert(r14Last.getBaseQualityString === "67778999:9;;<===>?@@@@AAAABCCCDE")


    val r1001First = collectedSam(("r1001", true, false))
    assert(r1001First.getFlags === 65)
    assert(r1001First.getContig === "e_coli:15000-66000")
    assert(r1001First.getAlignmentStart === 306)
    assert(r1001First.getCigarString === "37M1I38M")
    assert(r1001First.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTATTTGACGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9878775853EECBB@@@@?>===<;;9:99987776554")

    val r1001Last = collectedSam(("r1001", false, true))
    assert(r1001Last.getFlags === 129)
    assert(r1001Last.getContig === "e_coli:15000-66000")
    assert(r1001Last.getAlignmentStart === 450)
    assert(r1001Last.getCigarString === "75M")
    assert(r1001Last.getReadString === "CACTCCCCGCCGTTGCTCTTACTCGGATTCGTAAGCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTG")
    assert(r1001Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")


    val r1002First = collectedSam(("r1002", true, false))
    assert(r1002First.getFlags === 65)
    assert(r1002First.getContig === "e_coli:15000-66000")
    assert(r1002First.getAlignmentStart === 3182)
    assert(r1002First.getCigarString === "75M")
    assert(r1002First.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")

    val r1002Last = collectedSam(("r1002", false, true))
    assert(r1002Last.getFlags === 129)
    assert(r1002Last.getContig === "e_coli:15000-66000")
    assert(r1002Last.getAlignmentStart === 3328)
    assert(r1002Last.getCigarString === "20M1D54M")
    assert(r1002Last.getReadString === "ATAAACCGAAATATCTCTACATCTGAAATCTGATCGTTATGAAACACTTAATCTGATCGGTAAAAAAACAGATA")
    assert(r1002Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>9967968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")
  }

  // minimap2
  test("should returns correct fastq alignments' details by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r1000 = collectedSam("r1000")
    assert(r1000.getContig === "e_coli:15000-66000")
    assert(r1000.getAlignmentStart === 484)
    assert(r1000.getCigarString === "75M")
    assert(r1000.getReadString === "GCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTGAACCTCACAGAGGTCTTTTCTCGTTACCAGCGCC")
    assert(r1000.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "42M1I33M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGATCGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9887759853EECBB@@@@?>===<;;9:99987776554")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "26M1D34M1D13M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECB@@@@?>===<;9:99987776554")
  }

  test("should returns correct fasta alignments' details by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r1000 = collectedSam("r1000")
    assert(r1000.getContig === "e_coli:15000-66000")
    assert(r1000.getAlignmentStart === 484)
    assert(r1000.getCigarString === "75M")
    assert(r1000.getReadString === "GCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTGAACCTCACAGAGGTCTTTTCTCGTTACCAGCGCC")
    assert(r1000.getBaseQualityString === "*")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "50M1I25M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGACGACCTGATCTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "*")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "41M1D33M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "*")
  }

  test("should returns correct ifastq alignments' details by minimap2") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r1000First = collectedSam("r1000/1")
    assert(r1000First.getContig === "e_coli:15000-66000")
    assert(r1000First.getAlignmentStart === 484)
    assert(r1000First.getCigarString === "75M")
    assert(r1000First.getReadString === "GCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTGAACCTCACAGAGGTCTTTTCTCGTTACCAGCGCC")
    assert(r1000First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")

    val r1000Last = collectedSam("r1000/2")
    assert(r1000Last.getContig === "e_coli:15000-66000")
    assert(r1000Last.getAlignmentStart === 677)
    assert(r1000Last.getCigarString === "75M")
    assert(r1000Last.getReadString === "TATAGGGGGCCTCGGGTTGATGGTAAAATATCACTCGGGGCTTTTCTCTATCTGCCGTTCAGCTAATGCCTGAGA")
    assert(r1000Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")


    val r1001First = collectedSam("r1001/1")
    assert(r1001First.getContig === "e_coli:15000-66000")
    assert(r1001First.getAlignmentStart === 306)
    assert(r1001First.getCigarString === "37M1I38M")
    assert(r1001First.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTATTTGACGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9878775853EECBB@@@@?>===<;;9:99987776554")

    val r1001Last = collectedSam("r1001/2")
    assert(r1001Last.getContig === "e_coli:15000-66000")
    assert(r1001Last.getAlignmentStart === 450)
    assert(r1001Last.getCigarString === "75M")
    assert(r1001Last.getReadString === "CACTCCCCGCCGTTGCTCTTACTCGGATTCGTAAGCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTG")
    assert(r1001Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")


    val r1002First = collectedSam("r1002/1")
    assert(r1002First.getContig === "e_coli:15000-66000")
    assert(r1002First.getAlignmentStart === 3182)
    assert(r1002First.getCigarString === "75M")
    assert(r1002First.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")

    val r1002Last = collectedSam("r1002/2")
    assert(r1002Last.getContig === "e_coli:15000-66000")
    assert(r1002Last.getAlignmentStart === 3328)
    assert(r1002Last.getCigarString === "20M1D54M")
    assert(r1002Last.getReadString === "ATAAACCGAAATATCTCTACATCTGAAATCTGATCGTTATGAAACACTTAATCTGATCGGTAAAAAAACAGATA")
    assert(r1002Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>9967968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")
  }

  // bwa
  test("should returns correct fastq alignments' details by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.fqReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r33 = collectedSam("r33")
    assert(r33.getContig === "e_coli:15000-66000")
    assert(r33.getAlignmentStart === 49904)
    assert(r33.getCigarString === "5S30M")
    assert(r33.getReadString === "NNNNNTTTGGTTTGCCGCATGATCTGATGACCACG")
    assert(r33.getBaseQualityString === "45567778999:9;;<===>?@@@@AAAABCCCDE")

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "45567778999:9;;<===>?@@@@AAAABCCCDE")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "42M1I33M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGATCGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9887759853EECBB@@@@?>===<;;9:99987776554")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "26M1D34M1D13M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECB@@@@?>===<;9:99987776554")
  }

  test("should returns correct fasta alignments' details by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.faReadsPath, command)
    val collectedSam = sam.map(record => (record.getReadName, record)).collectAsMap()

    val r33 = collectedSam("r33")
    assert(r33.getContig === "e_coli:15000-66000")
    assert(r33.getAlignmentStart === 49904)
    assert(r33.getCigarString === "5S30M")
    assert(r33.getReadString === "NNNNNTTTGGTTTGCCGCATGATCTGATGACCACG")
    assert(r33.getBaseQualityString === "*")

    val r481 = collectedSam("r481")
    assert(r481.getContig === "e_coli:15000-66000")
    assert(r481.getAlignmentStart === 13938)
    assert(r481.getCigarString === "35M")
    assert(r481.getReadString === "TTCGGTAGAGAAAACGACCCGCAACGCCCTGCAAC")
    assert(r481.getBaseQualityString === "*")

    val r1001 = collectedSam("r1001")
    assert(r1001.getContig === "e_coli:15000-66000")
    assert(r1001.getAlignmentStart === 306)
    assert(r1001.getCigarString === "50M1I25M")
    assert(r1001.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTTTTGACGACCTGATCTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001.getBaseQualityString === "*")

    val r1002 = collectedSam("r1002")
    assert(r1002.getContig === "e_coli:15000-66000")
    assert(r1002.getAlignmentStart === 3182)
    assert(r1002.getCigarString === "41M1D33M")
    assert(r1002.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002.getBaseQualityString === "*")
  }

  test("should returns correct ifastq alignments' details by bwa") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val sam = SeqTenderAlignment.pipeReads(InputPaths.ifqReadsPath, command)
    val collectedSam = sam.map(record => (
      (// key
        record.getReadName,
        (record.getFlags & Constants.FIRST_SEGMENT_FLAG) == Constants.FIRST_SEGMENT_FLAG,
        (record.getFlags & Constants.LAST_SEGMENT_FLAG) == Constants.LAST_SEGMENT_FLAG
      ), record)
    ).collectAsMap()

    val r14First = collectedSam(("r14", true, false))
    assert(r14First.getFlags === 97)
    assert(r14First.getContig === "e_coli:15000-66000")
    assert(r14First.getAlignmentStart === 24900)
    assert(r14First.getCigarString === "35M")
    assert(r14First.getReadString === "GTAATTTGAGTAATGCCCACCAGTTCCATCACGAT")
    assert(r14First.getBaseQualityString === "EDCCCBAAAA@@@@?>===<;;9:99987776554")

    val r14Last = collectedSam(("r14", false, true))
    assert(r14Last.getFlags === 145)
    assert(r14Last.getContig === "e_coli:15000-66000")
    assert(r14Last.getAlignmentStart === 25105)
    assert(r14Last.getCigarString === "32M")
    assert(r14Last.getReadString === "CACGCACTTTATGCAGTGCTGCCAGCGCCGCC")
    assert(r14Last.getBaseQualityString === "67778999:9;;<===>?@@@@AAAABCCCDE")


    val r1001First = collectedSam(("r1001", true, false))
    assert(r1001First.getFlags === 65)
    assert(r1001First.getContig === "e_coli:15000-66000")
    assert(r1001First.getAlignmentStart === 306)
    assert(r1001First.getCigarString === "37M1I38M")
    assert(r1001First.getReadString === "GCGTTCAAAGAGCTTCTTTGATGGCGTGAAGAAGTTTATTTGACGACCTGACTCGCTAACCTCCCCAAAAGCCTGC")
    assert(r1001First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!9878775853EECBB@@@@?>===<;;9:99987776554")

    val r1001Last = collectedSam(("r1001", false, true))
    assert(r1001Last.getFlags === 129)
    assert(r1001Last.getContig === "e_coli:15000-66000")
    assert(r1001Last.getAlignmentStart === 450)
    assert(r1001Last.getCigarString === "75M")
    assert(r1001Last.getReadString === "CACTCCCCGCCGTTGCTCTTACTCGGATTCGTAAGCCGTGAAAACAGCAACCTCCGTCTGGCCAGTTCGGATGTG")
    assert(r1001Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")


    val r1002First = collectedSam(("r1002", true, false))
    assert(r1002First.getFlags === 65)
    assert(r1002First.getContig === "e_coli:15000-66000")
    assert(r1002First.getAlignmentStart === 3182)
    assert(r1002First.getCigarString === "75M")
    assert(r1002First.getReadString === "TGAATCGCTGGTTCCTGTTCTTGAGCAAAAAGCATTGAAACGCGAAAAGCCATTAATTTTCGGGATTGATATGCC")
    assert(r1002First.getBaseQualityString === "EDCCCBAAAAAAA@@@?>99697968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")

    val r1002Last = collectedSam(("r1002", false, true))
    assert(r1002Last.getFlags === 129)
    assert(r1002Last.getContig === "e_coli:15000-66000")
    assert(r1002Last.getAlignmentStart === 3328)
    assert(r1002Last.getCigarString === "20M1D54M")
    assert(r1002Last.getReadString === "ATAAACCGAAATATCTCTACATCTGAAATCTGATCGTTATGAAACACTTAATCTGATCGGTAAAAAAACAGATA")
    assert(r1002Last.getBaseQualityString === "EDCCCBAAAAAAA@@@?>9967968CDEBABBA!!988775853EECBB@@@@?>===<;;9:99987776554")
  }

}
