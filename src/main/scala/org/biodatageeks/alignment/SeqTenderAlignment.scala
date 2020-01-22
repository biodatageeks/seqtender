package org.biodatageeks.alignment

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.alignment.partitioners.{FastaRead, FastaReadInputFormat, FastqRead, FastqReadInputFormat}
import org.biodatageeks.shared.CustomRDDTextFunctions._
import org.biodatageeks.shared.IllegalFileExtensionException


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

    val readsExtension = AlignmentTools.getReadsExtension(readsPath)
    val rdds = if (readsExtension.equals(ReadsExtension.FQ)) {
      makeReadRddsFromFQ(readsPath)
    } else if (readsExtension.equals(ReadsExtension.FA)) {
      makeReadRddsFromFA(readsPath)
    } else throw IllegalFileExtensionException("Reads file isn't a fasta or fastq file")

    rdds.pipeRead(command)
  }

  def makeReadRddsFromFQ(inputPath: String)(implicit sparkSession: SparkSession): RDD[Text] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastqReadInputFormat],
        classOf[Text],
        classOf[FastqRead],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, FastqRead]]
      .mapPartitionsWithInputSplit { (_, iterator) =>

        // convert reads iterator to text one;
        // piping method requires text iterator
        //        iterator.map(it => convertFastqReadToText(it))
        iterator.map(it => it._2.toText)
      }
  }

  def makeReadRddsFromFA(inputPath: String)(implicit sparkSession: SparkSession): RDD[Text] = {
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastaReadInputFormat],
        classOf[Text],
        classOf[FastaRead],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, FastaRead]]
      .mapPartitionsWithInputSplit { (_, iterator) ⇒

        // map reads iterator to text one;
        // piping method requires text iterator
        iterator.map(it => it._2.toText)
      }
  }

}
