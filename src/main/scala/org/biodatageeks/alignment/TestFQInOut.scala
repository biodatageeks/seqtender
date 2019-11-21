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
    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    // /home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/data/bowtie2_index/e_coli
    /*val command = "docker run --rm -i " +
      "-v /home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/data/:/data " +
      "quay.io/biocontainers/bowtie2:2.3.4.3--py27h2d50403_0 " +
      "bowtie2 -x /data/bowtie2_index/e_coli - " */

    val commandBuilder = new CommandBuilder(
      readsPath = args(0),
      indexPath = args(1),
      tool = "minimap2"/*,
      interleaved = true*/
    )

    println(commandBuilder.getCommand)

    val alignment = SeqTenderAlignment
      .pipeReads(
        commandBuilder,
        sparkSession
      )

    //alignment.map(_.toString).collect().foreach(line => println(line))
    //    sparkSession.time(println(alignment.count()))

    saveRddToFile(sparkSession, alignment, args(2))
  }

  def saveRddToFile[T: ClassTag](sparkSession: SparkSession, rdd: RDD[SAMRecord], pathWrite: String): Unit = {
    val samTextWriter = new SAMTextWriter(new File(pathWrite))
    val collectedRdd = rdd.collect()

    samTextWriter.writeHeader(collectedRdd.head.getHeader.getTextHeader)
    collectedRdd.foreach(record => samTextWriter.writeAlignment(record))
    samTextWriter.close();
  }
}
