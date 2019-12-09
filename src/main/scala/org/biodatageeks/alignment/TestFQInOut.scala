package org.biodatageeks.alignment

import java.io.File

import htsjdk.samtools.{SAMRecord, SAMTextWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/*
1st arg - (input) fq/fa file - with reads
data/e_coli_1000.fq
2nd arg - (input) fa file - with reference genome
3rd arg - (output) sam file
*/
object TestFQInOut {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val commandBuilder = new CommandBuilder(
      readsPath = args(0),
      indexPath = args(1),
      tool = "bowtie2",
      interleaved = true
    )

    val alignment = SeqTenderAlignment
      .pipeReads(
        commandBuilder
      )

    //alignment.map(_.toString).collect().foreach(line => println(line))
    //    sparkSession.time(println(alignment.count()))

    saveRddToFile(alignment, args(2))
  }

  def saveRddToFile[T: ClassTag](rdd: RDD[SAMRecord], pathWrite: String): Unit = {
    val samTextWriter = new SAMTextWriter(new File(pathWrite))
    val collectedRdd = rdd.collect()

    samTextWriter.writeHeader(collectedRdd.head.getHeader.getTextHeader)
    collectedRdd.foreach(record => samTextWriter.writeAlignment(record))
    samTextWriter.close();
  }
}
