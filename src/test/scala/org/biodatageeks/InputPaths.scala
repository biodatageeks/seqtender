package org.biodatageeks

object InputPaths {
  val dataDirectory = "alignment"
  val readsDirectory = s"${dataDirectory}/reads"

  val referenceGenomePath: String = adjustPathToGitlab(
    getClass.getClassLoader.getResource(s"${dataDirectory}/bwa_index/e_coli_short.fa").getPath)
  val bwaIndex: String = referenceGenomePath
  val bwaIndexDirectory: String = bwaIndex.splitAt(bwaIndex.lastIndexOf("/") + 1)._1

  val bowtieIndex: String = changeIndexPath("bowtie_index")
  val bowtieIndexDirectory: String = bowtieIndex.splitAt(bowtieIndex.lastIndexOf("/") + 1)._1

  val bowtie2Index: String = changeIndexPath("bowtie2_index")
  val bowtie2IndexDirectory: String = bowtie2Index.splitAt(bowtie2Index.lastIndexOf("/") + 1)._1

  val gem3Index: String = changeIndexPath("gem3_index") + ".gem"
  val gem3IndexDirectory: String = gem3Index.splitAt(gem3Index.lastIndexOf("/") + 1)._1

  val magicBlastIndex: String = changeIndexPath("magicBlast_index")
  val magicBlastIndexDirectory: String = magicBlastIndex.splitAt(magicBlastIndex.lastIndexOf("/") + 1)._1

  val snapIndex: String = changeIndexPath("snap_index")
  val snapIndexDirectory: String = snapIndex.splitAt(snapIndex.lastIndexOf("/") + 1)._1

  val starIndex: String = changeIndexPath("star_index")
  val starIndexDirectory: String = starIndex.splitAt(starIndex.lastIndexOf("/") + 1)._1

  // reads
  val faReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fa").getPath
  val invalidSequenceFaReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-sequence.fa").getPath
  val invalidNameFaReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-name.fa").getPath

  val fqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.fq").getPath
  val invalidNameFqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-name.fq").getPath
  val invalidSequenceFqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-sequence.fq").getPath
  val invalidSeparatorFqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-separator.fq").getPath
  val invalidQualityFqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-quality.fq").getPath

  val ifqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_13.ifq").getPath
  val invalidNameIfqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-name.ifq").getPath
  val invalidSequenceIfqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-sequence.ifq").getPath
  val invalidSeparatorIfqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-separator.ifq").getPath
  val invalidQualityIfqReadsPath: String = getClass.getClassLoader.getResource(s"${readsDirectory}/e_coli_invalid-quality.ifq").getPath

  val invalidReadsPath: String = "invalid.txt"

  // it is necessary, because we can't get a path to nonspecific file
  private def changeIndexPath(finalDirectory: String): String = {
    val tmp = referenceGenomePath.replace("bwa_index", finalDirectory)
    val splitAt = tmp.length - 3
    tmp.splitAt(splitAt)._1
  }

  private def adjustPathToGitlab(path: String): String = {
    if(path.startsWith("/builds/biodatageeks"))
      return path.replace("/builds/biodatageeks", "/data/work/builds/biodatageeks")

    path
  }
}
