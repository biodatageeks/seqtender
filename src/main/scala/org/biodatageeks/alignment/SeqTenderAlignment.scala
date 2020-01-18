package org.biodatageeks.alignment

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.shared.CustomRDDTextFunctions._
import org.biodatageeks.shared.IllegalFileExtensionException
import org.seqdoop.hadoop_bam.{FastaInputFormat, FastqInputFormat, ReferenceFragment, SequencedFragment}


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
        classOf[FastqInputFormat],
        classOf[Text],
        classOf[SequencedFragment],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, SequencedFragment]]
      .mapPartitionsWithInputSplit { (_, iterator) =>

        // convert reads iterator to text one;
        // piping method requires text iterator
        iterator.map(it => convertFastqReadToText(it))
      }
  }

  def makeReadRddsFromFA(inputPath: String)(implicit sparkSession: SparkSession): RDD[Text] = {
    // todo: write custom fasta partitioner
    sparkSession.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastaInputFormat],
        classOf[Text],
        classOf[ReferenceFragment],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, ReferenceFragment]]
      .mapPartitionsWithInputSplit { (_, iterator) ⇒

        // convert reads iterator to text one;
        // piping method requires text iterator
        iterator.map(it => convertFastaReadToText(it))
      }
  }

  // convert single fastq read to text, which can be read by specified program
  private def convertFastqReadToText(read: (Text, SequencedFragment)): Text = {
    new Text(s"@${read._1}\n${read._2.getSequence.toString}\n+\n${read._2.getQuality.toString}")
  }

  // convert single fasta read to text, which can be read by specified program
  private def convertFastaReadToText(read: (Text, ReferenceFragment)): Text = {
    new Text(s">${read._2.getIndexSequence}\n${read._2.getSequence.toString}")
  }
}
