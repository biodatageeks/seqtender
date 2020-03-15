package org.biodatageeks.alignment

import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

object CommandBuilder {
  def buildCommand(readsExtension: ReadsExtension,
                   indexPath: String,
                   tool: String,
                   image: String = null,
                   interleaved: Boolean = false,
                   readGroupId: String,
                   readGroup: String): String = {

    val indexSplitPath: (String, String) = indexPath.splitAt(indexPath.lastIndexOf("/") + 1)
    val command = new StringBuilder("docker run --rm -i ")
    command.append(s"-v ${indexSplitPath._1}:/data ")

    val imageToCommand = if (image != null) image else getImage(tool)
    command.append(s"${imageToCommand} ")
    command.append(s"${toolBuilder(tool, indexSplitPath._2, readsExtension, interleaved, readGroupId, readGroup)}")

    command.toString()
  }

  private def getImage(tool: String): String = tool.toLowerCase() match {
    case Constants.bowtieToolName => Constants.defaultBowtieImage
    case Constants.bowtie2ToolName => Constants.defaultBowtie2Image
    case Constants.minimap2ToolName => Constants.defaultMinimap2Image
    case Constants.bwaToolName => Constants.defaultBWAImage
    case Constants.gem3ToolName => Constants.defaultGem3Image
    case Constants.magicBlastToolName => Constants.defaultMagicBlastImage
    case _ => throw new IllegalArgumentException("Unknown tool name")
  }

  private def toolBuilder(tool: String,
                          indexName: String,
                          readsExtension: ReadsExtension,
                          interleaved: Boolean,
                          readGroupId: String,
                          readGroup: String): String = tool.toLowerCase() match {

    case Constants.bowtieToolName => bowtieCommandBuilder(indexName, readsExtension, interleaved, readGroupId, readGroup)
    case Constants.bowtie2ToolName => bowtie2CommandBuilder(indexName, readsExtension, interleaved, readGroupId, readGroup)
    case Constants.minimap2ToolName => minimap2CommandBuilder(indexName, readGroupId, readGroup)
    case Constants.bwaToolName => bwaCommandBuilder(indexName, interleaved, readGroupId, readGroup)
    case Constants.gem3ToolName => gem3CommandBuilder(indexName, interleaved, readGroupId, readGroup)
    case Constants.magicBlastToolName => magicBlastCommandBuilder(indexName, readsExtension, interleaved)
    case _ => throw new IllegalArgumentException("Unknown tool name")
  }

  private def bowtieCommandBuilder(indexName: String,
                                   readsExtension: ReadsExtension,
                                   interleaved: Boolean,
                                   readGroupId: String,
                                   readGroup: String): String = {

    val command = new StringBuilder(s"${Constants.bowtieToolName} -S ")
    command.append(s"/data/$indexName ")

    command.append(s"--sam-RG ID:${getReadGroupId(readGroupId)} --sam-RG ${getReadGroup(readGroup)} ")

    if (readsExtension == ReadsExtension.FA) command.append("-f ")
    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def bowtie2CommandBuilder(indexName: String,
                                    readsExtension: ReadsExtension,
                                    interleaved: Boolean,
                                    readGroupId: String,
                                    readGroup: String): String = {

    val command = new StringBuilder(s"${Constants.bowtie2ToolName} -x ")
    command.append(s"/data/$indexName ")

    command.append(s"--rg-id ${getReadGroupId(readGroupId)} --rg ${getReadGroup(readGroup)} ")

    if (readsExtension == ReadsExtension.FA) command.append("-f ")
    if (interleaved) command.append("--interleaved ")

    command.append("- ").toString()
  }

  private def minimap2CommandBuilder(indexName: String, readGroupId: String, readGroup: String): String = {
    val command = new StringBuilder(s"${Constants.minimap2ToolName} -a -x map-ont ")

    command.append(s"""-R "@RG\\tID:${getReadGroupId(readGroupId)}\\t${getReadGroup(readGroup)}" """)

    command.append(s"/data/$indexName ")
    command.append("- ").toString()
  }

  private def bwaCommandBuilder(indexName: String, interleaved: Boolean, readGroupId: String, readGroup: String): String = {
    val command = new StringBuilder(s"${Constants.bwaToolName} mem ")

    command.append(s"""-R "@RG\\tID:${getReadGroupId(readGroupId)}\\t${getReadGroup(readGroup)}" """)

    if (interleaved) command.append("-p ")

    command.append(s"/data/$indexName ")
    command.append("- ").toString()
  }

  private def gem3CommandBuilder(indexName: String,
                                 interleaved: Boolean,
                                 readGroupId: String,
                                 readGroup: String): String = {

    val command = new StringBuilder(s"${Constants.gem3ToolName} -I ")
    command.append(s"/data/$indexName ")

    command.append(s"""-r "@RG\\tID:${getReadGroupId(readGroupId)}\\t${getReadGroup(readGroup)}" """)

    if (interleaved) command.append("-p ")

    command.append("- ").toString()
  }

  private def magicBlastCommandBuilder(indexName: String,
                                       readsExtension: ReadsExtension,
                                       interleaved: Boolean): String = {

    val command = new StringBuilder(s"${Constants.magicBlastToolName} -db ")
    command.append(s"/data/$indexName ")

    if (readsExtension == ReadsExtension.FQ || readsExtension == ReadsExtension.IFQ) command.append("-infmt fastq ")
    if (interleaved) command.append("-paired ")

    command.toString()
  }

  private def getReadGroupId(readGroupId: String): String = {
    if (readGroupId != null && !readGroupId.isEmpty)
      readGroupId
    else Constants.defaultRGId
  }

  private def getReadGroup(readGroup: String): String = {
    if (readGroup != null && !readGroup.isEmpty)
      readGroup
    else Constants.defaultRG
  }
}
