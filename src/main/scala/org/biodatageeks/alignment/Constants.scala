package org.biodatageeks.alignment

object Constants {
  val bowtieToolName: String = "bowtie"
  val bowtie2ToolName: String = "bowtie2"
  val minimap2ToolName: String = "minimap2"
  val bwaToolName: String = "bwa"

  val defaultBowtieImage = "quay.io/biocontainers/bowtie:1.2.2--py27h2d50403_1"
  val defaultBowtie2Image = "quay.io/biocontainers/bowtie2:2.3.4.3--py27h2d50403_0"
  val defaultMinimap2Image = "quay.io/biocontainers/minimap2:2.16--h84994c4_1"
  val defaultBWAImage = "quay.io/biocontainers/bwa:0.7.17--ha92aebf_3"

  final val defaultBowtieRGId = "test_id"
  final val defaultBowtieRG = "SM:Pool1"

  val faExtensions: List[String] = List("fa", "fasta", ".mfa", ".fna")
  val fqExtensions: List[String] = List("fq", "fastq", "ifq")
}
