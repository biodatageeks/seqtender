package org.biodatageeks.annotation

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder
import htsjdk.variant.vcf.VCFHeader
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD, VariantContextWithHeaderBDG}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.shared.CustomRDDTextFunctions._
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec, VCFHeaderReader, WrapSeekable}

import scala.collection.mutable

case class VariantContextWithHeaderRDD(headers:mutable.HashMap[String, VCFHeader],rdd: RDD[VariantContext]  )

object SeqTenderVCF {

  def pipeVCF(inputPath: String, command: String, spark: SparkSession): RDD[VariantContextWithHeaderBDG] = {
    spark
      .sparkContext.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    val rdds = makeVCFRDDs(spark, inputPath)
    rdds.pipeVCF(command)
  }

  def makeVCFRDDs(spark: SparkSession, inputPath: String): RDD[Text] = {
    val bc = broadCastVCFHeaders(inputPath, spark)
    spark
      .sparkContext
      .hadoopFile(inputPath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text], spark.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
        val file = inputSplit.asInstanceOf[FileSplit]
        val outputStream = new ByteArrayOutputStream()
        val vcfWriter = new VariantContextWriterBuilder()
          .clearOptions()
          .setOutputVCFStream(outputStream)
          .build()
        vcfWriter.writeHeader(bc.value(file.getPath.toString))
        vcfWriter.close()
        //first file chunk - do not preappend with a header
        if (file.getStart == 0)
          iterator.map(_._2)
        else {
          val bytes = outputStream.toByteArray()
          //preappend next partitions with header - but remove last sign ('/n')
          Iterator(new Text(bytes.take(bytes.length - 1))) ++ iterator.map(_._2)
        }
      }
  }

   def broadCastVCFHeaders(path: String, ss: SparkSession) = {
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val status = fs.globStatus(new Path(path))
    val headerMap = new mutable.HashMap[String, VCFHeader]()

    status.foreach(fs => headerMap(fs.getPath.toString) = VCFHeaderReader
      .readHeaderFrom(WrapSeekable.openPath(ss.sparkContext.hadoopConfiguration, new Path(fs.getPath.toUri))))

    ss.sparkContext.broadcast(headerMap)
  }
}
