package org.biodatageeks.alignment

import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

// todo: rename class name
class CommandBuilder(readsPath: String,
                      indexPath: String,
                      tool: String,
                      image: String = null,
                      interleaved: Boolean = false,
                      readGroupId : Option[String] = Some(Constants.defaultBowtieRGId),
                      readGroup: Option[String] = Some(Constants.defaultBowtieRG) ) {

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
    if (tool.toLowerCase() == Constants.bowtie2ToolName)
      return Constants.defaultBowtie2Image

    // todo: add another tools
    Constants.defaultBowtie2Image
  }

  private def toolBuilder(): String = {
    if (tool.toLowerCase() == Constants.bowtie2ToolName)
      return bowtie2CommandBuilder()

    // todo: add another tools
    bowtie2CommandBuilder()
  }

  private def bowtie2CommandBuilder(): String = {
    var command = "bowtie2 -x "
    command += s"/data/${indexSplitPath._2} "
    if(getReadsExtension == ReadsExtension.FA) command += "-f "
    if(interleaved) command += "--interleaved "
    readGroupId match{
      case Some(rgId) => command += s"--rg-id ${rgId} "
      case _ => None
    }
    readGroup match{
      case Some(rg) => command += s"--rg ${rg} "
      case _ => None
    }
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
