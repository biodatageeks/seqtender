package org.biodatageeks.alignment

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.spark.sql.SparkSession
import java.io.File

import htsjdk.samtools.reference.FastaSequenceFile
import org.seqdoop.hadoop_bam.FastaInputFormat.FastaRecordReader

object SeqTenderAlignment {

  def pipeAlignment(referencePath: String, readsPath: String, command: String/*, sparkSession: SparkSession*/) = {
    println("pipeAlignment")

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

  }

}
