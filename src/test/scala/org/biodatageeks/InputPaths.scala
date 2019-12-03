package org.biodatageeks

object InputPaths {
  val dataDirectory = "alignment"
  val readsDirectory = s"${dataDirectory}/reads"

  val referenceGenomePath: String = adjustPathToJenkins(
    getClass.getClassLoader.getResource(s"${dataDirectory}/bwa_index/e_coli_short.fa").getPath)
  val bwaIndex: String = referenceGenomePath
  val bwaIndexDirectory: String = bwaIndex.splitAt(bwaIndex.lastIndexOf("/") + 1)._1

  val bowtieIndex: String = changeIndexPath("bowtie_index")
  val bowtieIndexDirectory: String = bowtieIndex.splitAt(bowtieIndex.lastIndexOf("/") + 1)._1

  val bowtie2Index: String = changeIndexPath("bowtie2_index")
  val bowtie2IndexDirectory: String = bowtie2Index.splitAt(bowtie2Index.lastIndexOf("/") + 1)._1

  val faReadsPath: String = adjustPathToJenkins(getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fa").getPath)
  val fqReadsPath: String = adjustPathToJenkins(getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fq").getPath)
  val ifqReadsPath: String = adjustPathToJenkins(getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.ifq").getPath)

  // it is necessary, because we can't get a path to nonspecific file
  private def changeIndexPath(finalDirectory: String): String = {
    val tmp = referenceGenomePath.replace("bwa_index", finalDirectory)
    val splitAt = tmp.length - 3
    tmp.splitAt(splitAt)._1
  }

  private def adjustPathToJenkins(path: String): String = {
    if(path.contains("/var/jenkins_home"))
      return path.replace("/var/jenkins_home", "/data/samples/jenkins")

    path
  }
}
