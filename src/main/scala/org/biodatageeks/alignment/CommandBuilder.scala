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

  def getReadsPath: String = {
    readsPath
  }

  def getReadsExtension: ReadsExtension = {
    readsExtension
  }

  def getCommand: String = {
    var command = "docker run --rm -i "
    val imageToCommand = if (image != null) image else getImage

    command += s"-v ${indexSplitPath._1}:/data "
    command += s"${imageToCommand} ${toolBuilder()}"

    command
  }

    private def getImage: String = {
    val defaultBowtie2Image = "quay.io/biocontainers/bowtie2:2.3.4.3--py27h2d50403_0"

    if (tool == "bowtie2")
      return defaultBowtie2Image

    // todo: add another tools
    defaultBowtie2Image
  }

  private def toolBuilder(): String = {
    if (tool == "bowtie2")
      return bowtie2CommandBuilder()

    // todo: add another tools
    bowtie2CommandBuilder()
  }

  private def bowtie2CommandBuilder(): String = {
    var command = s"bowtie2 -x "
    command += s"/data/${indexSplitPath._2} "
    if(getReadsExtension == ReadsExtension.FA) {
      command += "-f "
    }
    if(interleaved) {
      command += "--interleaved "
    }

    command += "- "

    command
  }

  private def getExtension(filePath: String): ReadsExtension = {
    val extension = filePath.split("\\.").last
    val faExtensions: List[String] = List("fa", "fasta", ".mfa", ".fna")
    val fqExtensions: List[String] = List("fq", "fastq", "ifq")

    if (faExtensions.contains(extension.toLowerCase))
      ReadsExtension.FA
    else if (fqExtensions.contains(extension.toLowerCase))
      ReadsExtension.FQ
    else ReadsExtension.OTHER
  }
}
