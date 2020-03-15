package org.biodatageeks.alignment

object Constants {
  val bowtieToolName: String = "bowtie"
  val bowtie2ToolName: String = "bowtie2"
  val minimap2ToolName: String = "minimap2"
  val bwaToolName: String = "bwa"
  val gem3ToolName: String = "gem-mapper"
  val magicBlastToolName: String = "magicblast"

  val defaultBowtieImage = "quay.io/biocontainers/bowtie:1.2.3--py37hc9558a2_0"
  val defaultBowtie2Image = "quay.io/biocontainers/bowtie2:2.4.1--py38he513fc3_0"
  val defaultMinimap2Image = "quay.io/biocontainers/minimap2:2.17--h8b12597_1"
  val defaultBWAImage = "quay.io/biocontainers/bwa:0.7.17--hed695b0_7"
  val defaultGem3Image = "quay.io/biocontainers/gem3-mapper:3.6.1--h98de208_7"
  val defaultMagicBlastImage = "quay.io/biocontainers/magicblast:1.5.0--h2d02072_0"

  final val defaultRGId = "test_id"
  final val defaultRG = "SM:Pool1"

  val faExtensions: List[String] = List("fa", "fasta", "mfa", "fna", "mpfa", "fsa", "fas")
  val fqExtensions: List[String] = List("fq", "fastq")
  val ifqExtensions: List[String] = List("ifq")

  final val FIRST_SEGMENT_FLAG = 64
  final val LAST_SEGMENT_FLAG = 128
}
