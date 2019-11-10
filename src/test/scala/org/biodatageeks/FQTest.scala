package org.biodatageeks

import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.SeqTenderAlignment
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

class FQTest extends FunSuite with BeforeAndAfter with PrivateMethodTester {

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
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 40000)

    val rdds = SeqTenderAlignment.makeReadRdds(sparkSession, inputPath)

    assert(rdds.getNumPartitions === 2)
  }
}
