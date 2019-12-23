package org.biodatageeks.alignment

import org.apache.spark.sql.SparkSession

/*
1st arg - (input) fq/fa file - with reads
data/e_coli_1000.fq
2nd arg - (input) fa file - with reference genome
3rd arg - (output) sam file
*/
object TestFQMain {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val commandBuilder = new CommandBuilder(
      readsPath = args(0),
      indexPath = args(1),
      tool = "bowtie2",
      interleaved = true,
      readGroup="",
      readGroupId = ""
    )

    val alignment = SeqTenderAlignment
      .pipeReads(
        commandBuilder
      )

    //alignment.map(_.toString).collect().foreach(line => println(line))
    //    sparkSession.time(println(alignment.count()))

    AlignmentTools.saveRddToSAMFile(alignment, args(2))
  }
}
