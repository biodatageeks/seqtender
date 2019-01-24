package org.biodatageeks

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder
import htsjdk.variant.vcf.VCFHeader
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.util.{VCFHeaderReader, WrapSeekable}

import scala.collection.mutable
import org.biodatageeks.CustomFunctions._

object SeqTenderVCF {


  def pipeVCF(path:String, command:String, spark: SparkSession): RDD[VariantContext] ={
    val bc = broadCastVCFHeaders(path,spark)

    spark
      .sparkContext
      .hadoopFile(path,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],spark.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
        val file = inputSplit.asInstanceOf[FileSplit]
        val os = new ByteArrayOutputStream()
        val vcfWriter =   new VariantContextWriterBuilder()
          .clearOptions()
          .setOutputVCFStream( os )
          .build()
        vcfWriter.writeHeader(bc.value(file.getPath.toString))
        vcfWriter.close()
        //first file chunk - do not preappend with a header
        if(file.getStart == 0)
          iterator.map(_._2)
        else {
          val bytes = os.toByteArray()
          //preappend next partitions with header - but remove last sign ('/n')
          Iterator(new Text(bytes.take(bytes.length-1))) ++ iterator.map(_._2)
        }

      }
      .pipeVCF(command)
  }


  private def broadCastVCFHeaders(path:String, ss:SparkSession) ={
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val status = fs.globStatus(new Path(path))
    val headerMap = new mutable.HashMap[String,VCFHeader]()
    status
      .foreach(fs => headerMap(fs.getPath.toString)= VCFHeaderReader.readHeaderFrom(WrapSeekable.openPath(ss.sparkContext.hadoopConfiguration, new Path(fs.getPath.toUri))) )
    ss.sparkContext.broadcast(headerMap)
  }



}