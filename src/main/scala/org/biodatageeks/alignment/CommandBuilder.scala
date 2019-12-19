package org.biodatageeks.alignment

import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

// todo: rename class name
class CommandBuilder(readsPath: String,
                     indexPath: String,
                     tool: String,
                     image: String = null,
                     interleaved: Boolean = false,
                     readGroupId: String,
                     readGroup: String) {

  private val indexSplitPath: (String, String) = indexPath.splitAt(indexPath.lastIndexOf("/") + 1)
  private val readsExtension: ReadsExtension = getExtension(readsPath)
  private val command: String = buildCommand

  def getReadsPath: String = {
    readsPath
  }

  def getReadsExtension: ReadsExtension = {
    readsExtension
  }

  def getCommand: String = {
    command
  }

  private def buildCommand: String = {
    val command = new StringBuilder("docker run --rm -i ")
    command.append(s"-v ${indexSplitPath._1}:/data ")

    val imageToCommand = if (image != null) image else getImage
    command.append(s"${imageToCommand} ${toolBuilder()}")

    command.toString()
  }

  private def getImage: String = {
    val toolInLowerCase = tool.toLowerCase()

    if (toolInLowerCase == Constants.bowtieToolName)
      Constants.defaultBowtieImage
    else if (toolInLowerCase == Constants.bowtie2ToolName)
      Constants.defaultBowtie2Image
    else if (toolInLowerCase == Constants.minimap2ToolName)
      Constants.defaultMinimap2Image
    else if (toolInLowerCase == Constants.bwaToolName)
      Constants.defaultBWAImage
    else throw new IllegalArgumentException("Unknown tool name")
  }

  private def toolBuilder(): String = {
    val toolInLowerCase = tool.toLowerCase()

    if (toolInLowerCase == Constants.bowtieToolName)
      bowtieCommandBuilder()
    else if (toolInLowerCase == Constants.bowtie2ToolName)
      bowtie2CommandBuilder()
    else if (toolInLowerCase == Constants.minimap2ToolName)
      minimap2CommandBuilder()
    else if (toolInLowerCase == Constants.bwaToolName)
      bwaCommandBuilder()
    else throw new IllegalArgumentException("Unknown tool name")
  }

  private def bowtieCommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bowtieToolName} -S ")
    command.append(s"/data/${indexSplitPath._2} ")

    command.append(s"--sam-RG ID:$getReadGroupId --sam-RG $getReadGroup ")

    if (getReadsExtension == ReadsExtension.FA) command.append("-f ")
    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def bowtie2CommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bowtie2ToolName} -x ")
    command.append(s"/data/${indexSplitPath._2} ")

    command.append(s"--rg-id $getReadGroupId --rg $getReadGroup ")

    if (getReadsExtension == ReadsExtension.FA) command.append("-f ")
    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def minimap2CommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.minimap2ToolName} -a -x map-ont ")

    command.append(s"""-R "@RG\\tID:${getReadGroupId}\\t${getReadGroup}" """)

    command.append(s"/data/${indexSplitPath._2} ")
    command.append("- ").toString()
  }

  private def bwaCommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bwaToolName} mem ")

    command.append(s"""-R "@RG\\tID:$getReadGroupId\\t$getReadGroup" """)

    if (interleaved) command.append("-p ")

    command.append(s"/data/${indexSplitPath._2} ")
    command.append("- ").toString()
  }

  private def getReadGroupId: String = {
    if (readGroupId != null && !readGroupId.isEmpty)
      readGroupId
    else Constants.defaultBowtieRGId
  }

  private def getReadGroup: String = {
    if (readGroup != null && !readGroup.isEmpty)
      readGroup
    else Constants.defaultBowtieRG
  }

  private def getExtension(filePath: String): ReadsExtension = {
    val extension = filePath.split("\\.").last.toLowerCase

    if (Constants.faExtensions.contains(extension))
      ReadsExtension.FA
    else if (Constants.fqExtensions.contains(extension))
      ReadsExtension.FQ
    else ReadsExtension.OTHER
  }
}
