package org.biodatageeks.alignment

object Constants {
  val bowtieToolName: String = "bowtie"
  val bowtie2ToolName: String = "bowtie2"
  val minimap2ToolName: String = "minimap2"
  val bwaToolName: String = "bwa"

  val defaultBowtieImage = "quay.io/biocontainers/bowtie:1.2.3--py37hc9558a2_0"
  val defaultBowtie2Image = "quay.io/biocontainers/bowtie2:2.4.1--py38he513fc3_0"
  val defaultMinimap2Image = "quay.io/biocontainers/minimap2:2.17--h8b12597_1"
  val defaultBWAImage = "quay.io/biocontainers/bwa:0.7.17--hed695b0_7"

  final val defaultRGId = "test_id"
  final val defaultRG = "SM:Pool1"

  val faExtensions: List[String] = List("fa", "fasta", ".mfa", ".fna")
  val fqExtensions: List[String] = List("fq", "fastq")
  val ifqExtensions: List[String] = List("ifq")

  final val FIRST_SEGMENT_FLAG = 64
  final val LAST_SEGMENT_FLAG = 128
}
