package org.biodatageeks.alignment

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.partitioners._
import org.biodatageeks.utils.CustomRDDTextFunctions._
import org.biodatageeks.utils.IllegalFileExtensionException


object SeqTenderAlignment {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def pipeReads(readsPath: String, command: String)(implicit sparkSession: SparkSession): RDD[SAMRecord] = {
    logger.info(
      s"""
         |#########################
         |Running alignment process with command:
         |$command
         |with path:
         |$readsPath
         |########################
         |""".stripMargin)

    makeReadRDD(readsPath).pipeRead(command)
  }

  def makeReadRDD(readsPath: String)(implicit sparkSession: SparkSession): RDD[Text] = {
    val rdds: NewHadoopRDD[Text, WritableText] = AlignmentTools.getReadsExtension(readsPath) match {
      case ReadsExtension.FA => makeHadoopRDDFromFA(readsPath)
      case ReadsExtension.FQ => makeHadoopRDDFromFQ(readsPath)
      case ReadsExtension.IFQ => makeHadoopRDDFromIFQ(readsPath)
      case _ => throw IllegalFileExtensionException("Reads file isn't a fasta, fastq or interleaved fastq file")
    }
    rdds.mapPartitionsWithInputSplit { (_, iterator) =>
      // convert reads iterator to text one;
      // piping method requires text iterator
      iterator.map(it => it._2.toText)
    }
  }

  def makeHadoopRDDFromFQ(inputPath: String)(implicit sparkSession: SparkSession): NewHadoopRDD[Text, WritableText] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastqReadInputFormat],
        classOf[Text],
        classOf[FastqRead],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, WritableText]]
  }

  def makeHadoopRDDFromIFQ(inputPath: String)(implicit sparkSession: SparkSession): NewHadoopRDD[Text, WritableText] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[InterleavedFastqReadInputFormat],
        classOf[Text],
        classOf[InterleavedFastqRead],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, WritableText]]
  }

  def makeHadoopRDDFromFA(inputPath: String)(implicit sparkSession: SparkSession): NewHadoopRDD[Text, WritableText] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastaReadInputFormat],
        classOf[Text],
        classOf[FastaRead],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, WritableText]]
  }

}
