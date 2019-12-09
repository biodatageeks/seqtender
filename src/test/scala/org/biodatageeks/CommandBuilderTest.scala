package org.biodatageeks

import org.biodatageeks.alignment.{CommandBuilder, Constants}
import org.scalatest.FunSuite

class CommandBuilderTest extends FunSuite {

  // bowtie's tests
  test("should make correct bowtie command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtieIndexDirectory}:/data " +
      s"${Constants.defaultBowtieImage} " +
      "bowtie -S " +
      "/data/e_coli_short " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtieIndexDirectory}:/data " +
      s"${Constants.defaultBowtieImage} " +
      "bowtie -S /data/e_coli_short " +
      "--interleaved " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtieIndexDirectory}:/data " +
      s"${Constants.defaultBowtieImage} " +
      "bowtie -S " +
      "/data/e_coli_short " +
      "-f " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  // bowtie2's tests
  test("should make correct bowtie2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtie2IndexDirectory}:/data " +
      s"${Constants.defaultBowtie2Image} " +
      "bowtie2 -x " +
      s"/data/e_coli_short --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie2 command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtie2IndexDirectory}:/data " +
      s"${Constants.defaultBowtie2Image} " +
      "bowtie2 -x /data/e_coli_short " +
      s"--interleaved --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtie2IndexDirectory}:/data " +
      s"${Constants.defaultBowtie2Image} " +
      "bowtie2 -x " +
      "/data/e_coli_short " +
      s"-f --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  // minimap2's tests
  test("should make correct minimap2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultMinimap2Image} " +
      "minimap2 -a -x map-ont " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct minimap2 command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultMinimap2Image} " +
      "minimap2 -a -x map-ont " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct minimap2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultMinimap2Image} " +
      "minimap2 -a -x map-ont " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  // bwa's tests
  test("should make correct bwa command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultBWAImage} " +
      "bwa mem " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bwa command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultBWAImage} " +
      "bwa mem -p " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bwa command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bwaIndexDirectory}:/data " +
      s"${Constants.defaultBWAImage} " +
      "bwa mem " +
      "/data/e_coli_short.fa " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

}
