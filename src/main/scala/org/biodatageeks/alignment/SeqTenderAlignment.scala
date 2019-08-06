package org.biodatageeks.alignment

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}


object SeqTenderAlignment {

  def pipeAlignment(referencePath: String, readsPath: String, command: String, sparkSession: SparkSession): RDD[Text] = {
    val pathRead = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/text.txt"
    val pathWrite = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/int.txt"

    /*
    // open reference genome file
    val faReader = new FastaSequenceFile(new File(referencePath), false)
    println("reference name", faReader.nextSequence().length())
    */

    // sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 40000)

    sparkSession.sparkContext
      .newAPIHadoopFile(readsPath,
        classOf[FastqInputFormat],
        classOf[Text],
        classOf[SequencedFragment],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, SequencedFragment]]
      .mapPartitionsWithInputSplit { (inputSplit, iterator) =>
        val file = inputSplit.asInstanceOf[FileSplit]
        /*
          /*while (iterator.hasNext)
            println("x", iterator.next()._2)*/

          var retList = iterator.toList*/

        val mappedIterator = iterator.map(_._2)

        var tempList = List[Text]()
        mappedIterator.foreach(line => tempList = new Text(line.getSequence) :: tempList)

        val result = tempList.reverseIterator

        result

      }
  }

}
