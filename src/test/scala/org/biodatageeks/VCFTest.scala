package org.biodatageeks

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
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

  test("should return true when each partition (except first) has the headerLines") {
    sparkSession.sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)

    val headerLines =
      "##xfileformat=VCFv4.2\n##FILTER=<ID=PASS,Description=\"All filters passed\">\n##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">\n##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">\n##INFO=<ID=NS,Number=1,Type=Integer,Description=\"Number of Samples With Data\">\n##contig=<ID=11,assembly=b37,length=135006516>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample"

    val rdds = SeqTenderVCF.makeVCFRDDs(sparkSession, inputPath)
    var containHeader: Boolean = true
    rdds.foreachPartition(it => {
      // we don't have to check first partition, because it always has the header
      if (TaskContext.getPartitionId != 0) {
        var file = ArrayBuffer[String]()
        it.map(_.toString).foreach(it => {
          file += it
        })

        println("check")
        if (!file.contains(headerLines)) {
          println("not contain")
          containHeader = false
        }
        println(containHeader)
      }
    })

    println(containHeader)

    assert(containHeader === true)
  }

  test("should return number of elements in vcf rdd - number of lines in input file (without header)") {
    val vc = SeqTenderVCF
      .pipeVCF(
        inputPath,
        "docker run --rm -i biodatageeks/bdg-vt:latest vt decompose - ",
        sparkSession)

    assert(vc.count() === 2)
  }
}