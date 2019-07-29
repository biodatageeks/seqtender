package org.biodatageeks.alignment

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.spark.sql.SparkSession
import java.io.File

object SeqTenderAlignment {

  def pipeAlignment(path: String, command: String/*, sparkSession: SparkSession*/) = {
    println("pipeAlignment")

    // open file and write records
    val fqReader = new FastqReader(new File(path))
    var record: FastqRecord = null

    println(fqReader.next().getReadString())
    /*while(fqReader.hasNext) {
      println(fqReader.next())
    }*/

  }

}
