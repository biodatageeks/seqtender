package org.biodatageeks

import org.biodatageeks.alignment.{CommandBuilder, Constants}
import org.scalatest.FunSuite

class CommandBuilderTest extends FunSuite {

  test("should make correct bowtie2 command to align fq reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.fqReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
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
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtie2IndexDirectory}:/data " +
      s"${Constants.defaultBowtie2Image} " +
      "bowtie2 -x /data/e_coli_short " +
      s"--rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} --interleaved " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

  test("should make correct bowtie2 command to align fa reads") {
    val commandBuilder = new CommandBuilder(
      readsPath = InputPaths.faReadsPath,
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val correctCommand = "docker run --rm -i " +
      s"-v ${InputPaths.bowtie2IndexDirectory}:/data " +
      s"${Constants.defaultBowtie2Image} " +
      "bowtie2 -x " +
      "/data/e_coli_short " +
      s"--rg-id ${Constants.defaultBowtieRGId} --rg ${Constants.defaultBowtieRG} -f " +
      "- "

    assert(commandBuilder.getCommand === correctCommand)
  }

}
