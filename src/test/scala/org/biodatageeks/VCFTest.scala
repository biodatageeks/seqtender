package org.biodatageeks

import org.apache.spark.sql.SparkSession
import org.biodatageeks.SeqTenderVCF.makeVCFRDDs
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

import scala.collection.mutable.ArrayBuffer

class VCFTest extends FunSuite with BeforeAndAfter with PrivateMethodTester {

  val sparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()


  val inputPath = getClass.getClassLoader.getResource("vcfTest.vcf").getPath

  test("should make rdds on 2 partitions") {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)

    val rdds = SeqTenderVCF.makeVCFRDDs(sparkSession, inputPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("yyy") {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)

    val headerLines = Array("##FILTER=<ID=PASS,Description=\"All filters passed\">", "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
      "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">",
      "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">", "##INFO=<ID=NS,Number=1,Type=Integer,Description=\"Number of Samples With Data\">",
      "##contig=<ID=11,assembly=b37,length=135006516>", "#CHROM POS ID REF ALT QUAL FILTER INFO FORMAT sample",
      "#CHROM POS ID REF ALT QUAL FILTER INFO FORMAT sample",
    "##fileformat=VCFv4.2\n##FILTER=<ID=PASS,Description=\"All filters passed\">\n##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">\n##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">\n##INFO=<ID=NS,Number=1,Type=Integer,Description=\"Number of Samples With Data\">\n##contig=<ID=11,assembly=b37,length=135006516>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample")


    val rdds = SeqTenderVCF.makeVCFRDDs(sparkSession, inputPath)
    var containHeader: Boolean = false

    rdds.foreachPartition(it => {
      var header = ArrayBuffer[String]()
      it.map(_.toString).foreach(it => {
        header += it
      })

      println(s"header\n$header")
      println()
      println()
      headerLines.foreach(headerLine => {
        println()
        println(s"$headerLine, $containHeader")
        containHeader = header.contains(headerLine)
      })
    })

    println(containHeader)

    println("rdd:")
    rdds.map(_.toString).collect().foreach( it => println(s"xx $it"))

    assert(3 === 1 + 1)
  }
}