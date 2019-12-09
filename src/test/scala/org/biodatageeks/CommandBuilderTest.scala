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

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bowtie command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S /data/e_coli_short ")
    correctCommand.append("--interleaved ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bowtie command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append("-f ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  // bowtie2's tests
  test("should make correct bowtie2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x ")
    correctCommand.append(s"/data/e_coli_short --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bowtie2 command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x /data/e_coli_short ")
    correctCommand.append(s"--rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} --interleaved ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bowtie2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append(s"-f --rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  // minimap2's tests
  test("should make correct minimap2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct minimap2 command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct minimap2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  // bwa's tests
  test("should make correct bwa command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bwa command to align interleaved fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.ifqReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem -p ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString)
  }

  test("should make correct bwa command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem ")
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(commandBuilder.getCommand === correctCommand.toString())
  }

}
