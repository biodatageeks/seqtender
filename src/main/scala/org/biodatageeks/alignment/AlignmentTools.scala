package org.biodatageeks.alignment

import java.io.File

import htsjdk.samtools.{SAMRecord, SAMTextWriter}
import org.apache.spark.rdd.RDD
import org.biodatageeks.alignment.ReadsExtension.ReadsExtension

import scala.reflect.ClassTag

object AlignmentTools {
  def getReadsExtension(filePath: String): ReadsExtension = {
    val extension = filePath.split("\\.").last.toLowerCase

    if (Constants.faExtensions.contains(extension))
      ReadsExtension.FA
    else if (Constants.fqExtensions.contains(extension))
      ReadsExtension.FQ
    else if (Constants.ifqExtensions.contains(extension))
      ReadsExtension.IFQ
    else ReadsExtension.OTHER
  }

  def saveRddToSAMFile[T: ClassTag](rdd: RDD[SAMRecord], pathWrite: String): Unit = {
    val samTextWriter = new SAMTextWriter(new File(pathWrite))
    val collectedRdd = rdd.collect()

    samTextWriter.writeHeader(collectedRdd.head.getHeader.getTextHeader)
    collectedRdd.foreach(record => samTextWriter.writeAlignment(record))
    samTextWriter.close();
  }
}

object ReadsExtension extends Enumeration {
  type ReadsExtension = Value
  val FA, FQ, IFQ, OTHER = Value
}
