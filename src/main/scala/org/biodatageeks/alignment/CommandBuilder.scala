package org.biodatageeks.alignment

import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

// todo: rename class name
class CommandBuilder(readsPath: String,
                     indexPath: String,
                     tool: String,
                     image: String = null,
                     interleaved: Boolean = false,
                     readGroupId: String = Constants.defaultBowtieRGId,
                     readGroup: String = Constants.defaultBowtieRG) {

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
      return Constants.defaultBowtieImage
    else if (toolInLowerCase == Constants.bowtie2ToolName)
      return Constants.defaultBowtie2Image
    else if (toolInLowerCase == Constants.minimap2ToolName)
      return Constants.defaultMinimap2Image
    else if (toolInLowerCase == Constants.bwaToolName)
      return Constants.defaultBWAImage

    // todo: throw exception when tool name is unknown
    Constants.defaultBowtie2Image
  }

  private def toolBuilder(): String = {
    val toolInLowerCase = tool.toLowerCase()

    if (toolInLowerCase == Constants.bowtieToolName)
      return bowtieCommandBuilder()
    else if (toolInLowerCase == Constants.bowtie2ToolName)
      return bowtie2CommandBuilder()
    else if (toolInLowerCase == Constants.minimap2ToolName)
      return minimap2CommandBuilder()
    else if (toolInLowerCase == Constants.bwaToolName)
      return bwaCommandBuilder()

    // todo: throw exception when tool name is unknown
    bowtie2CommandBuilder()
  }

  private def bowtieCommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bowtieToolName} -S ")
    command.append(s"/data/${indexSplitPath._2} ")

    if (getReadsExtension == ReadsExtension.FA) command.append("-f ")

    if (readGroupId != null && !readGroupId.isEmpty && readGroup != null && !readGroup.isEmpty)
      command.append(s"--sam-RG ID:$readGroupId --sam-RG $readGroup ")

    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def bowtie2CommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bowtie2ToolName} -x ")
    command.append(s"/data/${indexSplitPath._2} ")

    if (getReadsExtension == ReadsExtension.FA) command.append("-f ")

    // I changed this condition, because read group cannot exist without read group id
    if (readGroupId != null && !readGroupId.isEmpty && readGroup != null && !readGroup.isEmpty)
      command.append(s"--rg-id $readGroupId --rg $readGroup ")

    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def minimap2CommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.minimap2ToolName} -a -x map-ont ")

    if (readGroupId != null && !readGroupId.isEmpty && readGroup != null && !readGroup.isEmpty)
      command.append(s"""-R "@RG\\tID:$readGroupId\\t$readGroup" """)

    command.append(s"/data/${indexSplitPath._2} ")
    command.append("- ").toString()
  }

  private def bwaCommandBuilder(): String = {
    val command = new StringBuilder(s"${Constants.bwaToolName} mem ")

    if (readGroupId != null && !readGroupId.isEmpty && readGroup != null && !readGroup.isEmpty)
      command.append(s"""-R "@RG\\tID:$readGroupId\\t$readGroup" """)

    if (interleaved) command.append("-p ")

    command.append(s"/data/${indexSplitPath._2} ")
    command.append("- ").toString()
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
