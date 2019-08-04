package org.biodatageeks.alignment

import java.io.File

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}


object SeqTenderAlignment {

  def pipeAlignment(referencePath: String, readsPath: String, command: String, sparkSession: SparkSession)/*: RDD[Text]*/ = {
    val pathRead = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/text.txt"
    val pathWrite = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/int.txt"

    /*
    // open reference genome file
    val faReader = new FastaSequenceFile(new File(referencePath), false)
    println("reference name", faReader.nextSequence().length())
    */


    // open file and read records from file with reads
    val fqReader = new FastqReader(new File(readsPath))
    var record: FastqRecord = null
    record = fqReader.next()
    println(record.getReadName())

    // println("first read", fqReader.next().getReadString())
    /*while(fqReader.hasNext) {
      println(fqReader.next())
    }*/
/*
    val tmp = sparkSession.sparkContext
      .newAPIHadoopFile(readsPath,
        classOf[FastqInputFormat],
        classOf[Text],
        classOf[SequencedFragment],
        sparkSession.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, SequencedFragment]]
      .map{read => read._2}

    tmp.collect().foreach(line => println(line.getSequence.toString))

    tmp*/
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

    /*
    println("save ->")
    tmp.saveAsTextFile(pathWrite)*/

    /*
    sparkSession.sparkContext
      .hadoopFile(pathRead,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        sparkSession.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit { (inputSplit, iterator) =>
        val file = inputSplit.asInstanceOf[FileSplit]
        /*
          /*while (iterator.hasNext)
            println("x", iterator.next()._2)*/

          var retList = iterator.toList*/

        val mappedIterator = iterator.map(_._2)

        var tempList = List[Text]()
        mappedIterator.foreach(line => tempList = new Text(line + "xxx") :: tempList)

        var result = Iterator[Text]()
        result = tempList.reverseIterator ++ Seq(new Text("4"), new Text("6"))

        result

      }
      */
  }

}
