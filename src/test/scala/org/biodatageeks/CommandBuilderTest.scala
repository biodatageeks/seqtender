package org.biodatageeks

import org.biodatageeks.alignment.{AlignmentTools, CommandBuilder, Constants}
import org.scalatest.FunSuite

class CommandBuilderTest extends FunSuite {

  // bowtie's tests
  test("should make correct bowtie command to align fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append(s"--sam-RG ID:${Constants.defaultRGId} --sam-RG ${Constants.defaultRG} ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bowtie command to align interleaved fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      interleaved = true,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S /data/e_coli_short ")
    correctCommand.append(s"--sam-RG ID:${Constants.defaultRGId} --sam-RG ${Constants.defaultRG} --interleaved ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bowtie command to align fa reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtieIndex,
      tool = Constants.bowtieToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtieIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtieImage} ")
    correctCommand.append("bowtie -S ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append(s"--sam-RG ID:${Constants.defaultRGId} --sam-RG ${Constants.defaultRG} -f ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  // bowtie2's tests
  test("should make correct bowtie2 command to align fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x ")
    correctCommand.append(s"/data/e_coli_short --rg-id ${Constants.defaultRGId} --rg ${Constants.defaultRG} ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bowtie2 command to align interleaved fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      interleaved = true,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x /data/e_coli_short ")
    correctCommand.append(s"--rg-id ${Constants.defaultRGId} --rg ${Constants.defaultRG} --interleaved ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bowtie2 command to align fa reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bowtie2Index,
      tool = Constants.bowtie2ToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bowtie2IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBowtie2Image} ")
    correctCommand.append("bowtie2 -x ")
    correctCommand.append("/data/e_coli_short ")
    correctCommand.append(s"--rg-id ${Constants.defaultRGId} --rg ${Constants.defaultRG} -f ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  // minimap2's tests
  test("should make correct minimap2 command to align fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct minimap2 command to align interleaved fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      interleaved = true,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct minimap2 command to align fa reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.referenceGenomePath,
      tool = Constants.minimap2ToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultMinimap2Image} ")
    correctCommand.append("minimap2 -a -x map-ont ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  // bwa's tests
  test("should make correct bwa command to align fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bwa command to align interleaved fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      interleaved = true,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" -p """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct bwa command to align fa reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.bwaIndex,
      tool = Constants.bwaToolName,
      readGroup = Constants.defaultRG,
      readGroupId = Constants.defaultRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.bwaIndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultBWAImage} ")
    correctCommand.append("bwa mem ")
    correctCommand.append(s"""-R "@RG\\tID:${Constants.defaultRGId}\\t${Constants.defaultRG}" """)
    correctCommand.append("/data/e_coli_short.fa ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString())
  }

  // gem3's tests
  test("should make correct gem3 command to align fq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
      indexPath = InputPaths.gem3Index,
      tool = Constants.gem3ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.gem3IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultGem3Image} ")
    correctCommand.append("gem-mapper -I ")
    correctCommand.append(s"/data/e_coli_short.gem ")
    correctCommand.append(s"""-r "@RG\\tID:${Constants.defaultBowtieRGId}\\t${Constants.defaultBowtieRG}" """)
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct gem3 command to align fa reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.faReadsPath),
      indexPath = InputPaths.gem3Index,
      tool = Constants.gem3ToolName,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.gem3IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultGem3Image} ")
    correctCommand.append("gem-mapper -I ")
    correctCommand.append(s"/data/e_coli_short.gem ")
    correctCommand.append(s"""-r "@RG\\tID:${Constants.defaultBowtieRGId}\\t${Constants.defaultBowtieRG}" """)
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  test("should make correct gem3 command to align ifq reads") {
    val command = CommandBuilder.buildCommand(
      readsExtension = AlignmentTools.getReadsExtension(InputPaths.ifqReadsPath),
      indexPath = InputPaths.gem3Index,
      tool = Constants.gem3ToolName,
      interleaved = true,
      readGroup = Constants.defaultBowtieRG,
      readGroupId = Constants.defaultBowtieRGId
    )

    val correctCommand = new StringBuilder("docker run --rm -i ")
    correctCommand.append(s"-v ${InputPaths.gem3IndexDirectory}:/data ")
    correctCommand.append(s"${Constants.defaultGem3Image} ")
    correctCommand.append("gem-mapper -I ")
    correctCommand.append(s"/data/e_coli_short.gem ")
    correctCommand.append(s"""-r "@RG\\tID:${Constants.defaultBowtieRGId}\\t${Constants.defaultBowtieRG}" """)
    correctCommand.append("-p ")
    correctCommand.append("- ")

    assert(command === correctCommand.toString)
  }

  // exception
  test("should throw IllegalArgumentException when try build command with unknown tool name") {
    val thrown = intercept[IllegalArgumentException] {
      CommandBuilder.buildCommand(
        readsExtension = AlignmentTools.getReadsExtension(InputPaths.fqReadsPath),
        indexPath = InputPaths.bowtieIndex,
        tool = "unknownToolName",
        readGroup = Constants.defaultRG,
        readGroupId = Constants.defaultRGId
      )
    }

    assert(thrown.getMessage === "Unknown tool name")
  }
}
