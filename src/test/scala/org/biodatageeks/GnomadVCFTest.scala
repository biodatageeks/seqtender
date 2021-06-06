package org.biodatageeks

import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.vcf.{VCFCodec, VCFFileReader, VCFHeader}
import org.apache.spark.rdd.VariantContextWithHeader
import org.apache.spark.sql.SparkSession
import org.biodatageeks.annotation.SeqTenderVCF
import org.scalatest.FunSuite

import java.io.{File, FileInputStream}

class GnomadVCFTest extends FunSuite {

  val inputPath: String = getClass.getClassLoader.getResource("vcf/gnomad.vcf").getPath
//  val inputPath = "/Users/mwiewior/Downloads/test_gnomad.vcf"
  val inputFile = new File(inputPath)
  val vcfReader = new VCFFileReader(inputFile, false)
//  implicit val sparkSession: SparkSession = SparkSession
//    .builder()
//    .master("local[1]")
//    .getOrCreate()

//  test("Read GNOMAD VCF"){
//    val vcfIterator = vcfReader.iterator()
//    val header = vcfReader.getFileHeader
//    while(vcfIterator.hasNext){
//      val variant = vcfIterator.next()
//      println(variant.toString)
//    }
//
//  }

  test("Decode stream"){
    val fileStream = new FileInputStream(inputPath)
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(fileStream))
    val codec = new VCFCodec()

    // read the header
    val header = codec.readActualHeader(lri).asInstanceOf[VCFHeader]

    while (lri.hasNext) {
      val vc = new VariantContextWithHeader(codec.decode(lri.next()), header)
      println(vc.getVariantContext.toString)
    }
  }
//
//  test("SeqTenderVCF"){
//    val vc = SeqTenderVCF
//      .pipeVCF(
//        inputPath,
//        "cat",
//        sparkSession)
//    println(vc.count() )
//    sparkSession.stop()
//  }

//  test("Record size") {
//    val fullRecord = "chr1	644694	rs1207020613	G	A	141	PASS	AF=1.17192e-05;AF_ami=0;AF_oth=0;AF_afr=0.000126775;AF_sas=0;AF_asj=0;nhomalt=0;AF_fin=0;AF_amr=0;AF_nfe=0;AF_eas=0;CSQ=A|intron_variant&non_coding_transcript_variant|MODIFIER|AL669831.3|ENSG00000230021|Transcript|ENST00000635509|processed_transcript||2/3|ENST00000635509.2:n.313-43117C>T||||||||TGP-PL|HET|G||-1||SNV|Clone_based_ensembl_"
//    val b: Array[Byte] = fullRecord.getBytes("UTF-16")
//    println(b.length)
//  }
}
