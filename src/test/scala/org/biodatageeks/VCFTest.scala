package org.biodatageeks

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

import scala.collection.mutable.ArrayBuffer

class VCFTest extends FunSuite with BeforeAndAfter with PrivateMethodTester {

  val sparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  val inputPath = getClass.getClassLoader.getResource("vcfTest.vcf").getPath

  before {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
  }

  test("should make rdds on 2 partitions") {
    val rdds = SeqTenderVCF.makeVCFRDDs(sparkSession, inputPath)

    assert(rdds.getNumPartitions === 2)
  }

  test("should return true when each partition has the headerLines - header was added to partitions in makeVCFRDDs method") {
    val headerLines =
      "##fileformat=VCFv4.2\n##FILTER=<ID=PASS,Description=\"All filters passed\">\n##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">\n##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">\n##INFO=<ID=NS,Number=1,Type=Integer,Description=\"Number of Samples With Data\">\n##contig=<ID=11,assembly=b37,length=135006516>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample"

    val rdds = SeqTenderVCF.makeVCFRDDs(sparkSession, inputPath)
    val notContainHeader = sparkSession.sparkContext.longAccumulator
    rdds.foreachPartition(it => {
      // we don't have to check first partition, because it always has the header (by default)
      if (TaskContext.getPartitionId != 0) {
        var file = ArrayBuffer[String]()
        it.map(_.toString).foreach(it => {
          file += it
        })

        if (!file.contains(headerLines))
          notContainHeader.add(1)
      }
    })

    assert(notContainHeader.value === 0)
  }

  test("should return number of biallelic variants in vcf rdd and any variant mustn't be multiallelic") {
    val vc = SeqTenderVCF
      .pipeVCF(
        inputPath,
        "docker run --rm -i biodatageeks/bdg-vt:latest vt decompose - ",
        sparkSession)

    var areMultiallelicVariants = false
    vc.collect().foreach(v => {
      if(v.getAlleles.size() != 1)
        areMultiallelicVariants = true
    })

    assert(areMultiallelicVariants, false)
    assert(vc.count() === 24)
  }
}