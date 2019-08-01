package org.biodatageeks.alignment

import java.util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SeqTenderAlignment {

  def pipeAlignment(referencePath: String, readsPath: String, command: String, sparkSession: SparkSession)/*: RDD[Text]*/ = {
    val pathRead = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/text.txt"
    val pathWrite = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/int.txt"
    /*
    // open reference genome file
    val faReader = new FastaSequenceFile(new File(referencePath), false)
    println("reference name", faReader.nextSequence().length())

    // open file and read records from file with reads
    val fqReader = new FastqReader(new File(readsPath))
    var record: FastqRecord = null

    println("first read", fqReader.next().getReadString())
    /*while(fqReader.hasNext) {
      println(fqReader.next())
    }*/
    */

    /*val tmp = sparkSession.sparkContext
      .hadoopFile(pathRead,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        sparkSession.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .map{long => long._2.toString}

    tmp.collect().foreach(line => println(line))

    println("save ->")
    tmp.saveAsTextFile(pathWrite)*/

    sparkSession.sparkContext
      .hadoopFile(pathRead,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        sparkSession.sparkContext.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit{ (inputSplit, iterator) =>
        val file = inputSplit.asInstanceOf[FileSplit]
        println("length", file.getLength)

        // println("it", iterator)

        /*while(iterator.hasNext)
          println("x", iterator.next()._2)*/

        var retList = iterator.toList

        retList.foreach(println("??", _))

        // retList.map(_._2).iterator

        println("next", retList)

        retList.map(_._2).iterator
        //retList.iterator


      }.map( _.toString).collect().foreach(line => println(line))

    /*val intRdd = sparkSession.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    intRdd.saveAsTextFile(pathWrite)*/

    /*
    sparkSession.sparkContext
      .textFile(pathRead)*/

  }

}
