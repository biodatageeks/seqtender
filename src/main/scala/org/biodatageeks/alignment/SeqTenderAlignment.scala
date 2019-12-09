package org.biodatageeks.alignment

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.CustomRDDTextFunctions._
import org.biodatageeks.conf.InternalParams
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}


object SeqTenderAlignment {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def pipeReads(readsDescription: CommandBuilder)(implicit sparkSession: SparkSession): RDD[SAMRecord] = {

    logger.info(
      s"""
         |#########################
         |Runnig alignment process with command:
         |${readsDescription.getCommand}
         |with path:
         |${readsDescription.getReadsPath}
         |########################
         |""".stripMargin)
    val rdds = if(readsDescription.getReadsExtension.equals(ReadsExtension.FQ)) {
      makeReadRddsFromFQ(readsDescription.getReadsPath)
    } else /*if (readsDescription.getReadsExtension.equals(ReadsExtension.FA))*/ {
      makeReadRddsFromFA(readsDescription.getReadsPath)
    } // todo: throw exception when extension isn't fa or fq

    rdds.pipeRead(readsDescription.getCommand)
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
        iterator.map(it => convertReadToText(it))
      }
  }

  def makeReadRddsFromFA(inputPath: String)(implicit sparkSession: SparkSession): RDD[Text] = {
    sparkSession
      .sparkContext
      .hadoopFile(inputPath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text], sparkSession.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit { (_, iterator) ⇒
        iterator.map(_._2)
      }
  }

  // convert single read to text, which can be read by specified program
  private def convertReadToText(read: (Text, SequencedFragment)): Text = {
    new Text(s"@${read._1}\n${read._2.getSequence.toString}\n+\n${read._2.getQuality.toString}")
  }
}
