package org.biodatageeks

import org.biodatageeks.alignment.{CommandBuilder, Constants}
import org.scalatest.FunSuite

class CommandBuilderTest extends FunSuite {
  val dataDirectory = "alignment"
  val readsDirectory = s"${dataDirectory}/reads"

  val referenceGenomePath: String = getClass.getClassLoader.getResource(s"${dataDirectory}/bwa_index/e_coli_short.fa").getPath
  val bwaIndex: String = referenceGenomePath
  val bwaIndexDirectory: String = bwaIndex.splitAt(bwaIndex.lastIndexOf("/") + 1)._1

  val bowtieIndex: String = changeIndexPath("bowtie_index")
  val bowtieIndexDirectory: String = bowtieIndex.splitAt(bowtieIndex.lastIndexOf("/") + 1)._1

  val bowtie2Index: String = changeIndexPath("bowtie2_index")
  val bowtie2IndexDirectory: String = bowtie2Index.splitAt(bowtie2Index.lastIndexOf("/") + 1)._1

  val faReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fa").getPath
  val fqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fq").getPath
  val ifqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.ifq").getPath

  // it is necessary, because we can't get a path to nonspecific file
  private def changeIndexPath(finalDirectory: String): String = {
    val tmp = referenceGenomePath.replace("bwa_index", finalDirectory)
    val splitAt = tmp.length - 3
    tmp.splitAt(splitAt)._1
  }


  test("should make correct bowtie2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = fqReadsPath,
      indexPath = bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v $bowtie2IndexDirectory:/data " +
      Constants.defaultBowtie2Image +
      "bowtie2 -x " +
      "/data/e_coli_short " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie2 command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = ifqReadsPath,
      indexPath = bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true
    )

    val correctCommand = "docker run --rm -i " +
      s"-v $bowtie2IndexDirectory:/data " +
      Constants.defaultBowtie2Image +
      "bowtie2 -x /data/e_coli_short " +
      "--interleaved " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = faReadsPath,
      indexPath = bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v $bowtie2IndexDirectory:/data " +
      Constants.defaultBowtie2Image +
      "bowtie2 -x " +
      "/data/e_coli_short " +
      "-f " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

}
