package org.biodatageeks.alignment

import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

// todo: rename class name
class CommandBuilder(readsPath: String,
                      indexPath: String,
                      tool: String,
                      image: String = null,
                      interleaved: Boolean = false) {

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
    var command = "docker run --rm -i "
    val imageToCommand = if (image != null) image else getImage

    command += s"-v ${indexSplitPath._1}:/data "
    command += s"${imageToCommand} ${toolBuilder()}"

    command
  }

    private def getImage: String = {
      if (tool.toLowerCase() == Constants.bowtieToolName)
        return Constants.defaultBowtieImage
      else if (tool.toLowerCase() == Constants.bowtie2ToolName)
        return Constants.defaultBowtie2Image
      else if (tool.toLowerCase() == Constants.minimap2ToolName)
        return Constants.defaultMinimap2Image
      else if (tool.toLowerCase() == Constants.bwaToolName)
        return Constants.defaultBWAImage

      Constants.defaultBowtie2Image
  }

  private def toolBuilder(): String = {
    if (tool.toLowerCase() == Constants.bowtieToolName)
      return bowtieCommandBuilder()
    else if (tool.toLowerCase() == Constants.bowtie2ToolName)
      return bowtie2CommandBuilder()
    else if (tool.toLowerCase() == Constants.minimap2ToolName)
      return minimap2CommandBuilder()
    else if (tool.toLowerCase() == Constants.bwaToolName)
      return bwaCommandBuilder()

    bowtie2CommandBuilder()
  }

  // todo: not works -> exception while generating SAMRecords
  private def bowtieCommandBuilder(): String = {
    var command = "bowtie "
    command += s"/data/${indexSplitPath._2} "
    if(getReadsExtension == ReadsExtension.FA) command += "-f "
    if(interleaved) command += "--interleaved "
    command += "- "
    command
  }

  // works
  private def bowtie2CommandBuilder(): String = {
    var command = "bowtie2 -x " // should change hardcoded 'bowtie2' to ${Constants.bowtie2ToolName} ?
    command += s"/data/${indexSplitPath._2} "
    if(getReadsExtension == ReadsExtension.FA) command += "-f "
    if(interleaved) command += "--interleaved "
    command += "- "
    command
  }

  // works
  private def minimap2CommandBuilder(): String = {
    var command = "minimap2 -a -x map-ont "
    command += s"/data/${indexSplitPath._2} "
    command += "- "
    command
  }

  // works
  private def bwaCommandBuilder(): String = {
    var command = "bwa mem "
    if(interleaved) command += "-p "
    command += s"/data/${indexSplitPath._2} "
    command += "- "
    command
  }


  private def getExtension(filePath: String): ReadsExtension = {
    val extension = filePath.split("\\.").last

    if (Constants.faExtensions.contains(extension.toLowerCase))
      ReadsExtension.FA
    else if (Constants.fqExtensions.contains(extension.toLowerCase))
      ReadsExtension.FQ
    else ReadsExtension.OTHER
  }
}
