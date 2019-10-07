package org.biodatageeks.alignment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/*
1st arg - (input) fq file - with reads
data/e_coli_1000.fq
2nd arg - (output) txt file
*/
object TestFQInOut {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val alignment = SeqTenderAlignment
      .pipeReads(
        args(0),
        "",
        sparkSession
      )

    alignment.map(_.toString).collect().foreach(line => println(line))

    saveRddToFile(sparkSession, alignment, args(1))
  }

  def saveRddToFile[T: ClassTag](sparkSession: SparkSession, rdd: RDD[T], pathWrite: String): Unit = {
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val outputStream = fileSystem.create(new Path(pathWrite))
    rdd.map(_.toString).collect().foreach(line => outputStream.writeBytes(s"\n$line"))
    outputStream.close()
  }
}
