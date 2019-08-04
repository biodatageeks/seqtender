package org.biodatageeks.alignment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object TestFQInOut {
  val pathWrite = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/output.txt"

  def main(args: Array[String]): Unit = {
    args.foreach(tmp => println(tmp))
    println("Fuck you, dear")

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    println("Spark session built")

    val alignment = SeqTenderAlignment
      .pipeAlignment(
        args(0), args(1),
        "",
        sparkSession
      )

     alignment.map(_.toString).collect().foreach(line => println(line))

    //val intRdd = sparkSession.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    // alignment.coalesce(1).saveAsTextFile(pathWrite)

    saveRddToFile(sparkSession, alignment)

  }

  def saveRddToFile[T: ClassTag](sparkSession: SparkSession, rdd: RDD[T]): Unit = {
    val filesystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val outputStream = filesystem.create(new Path(pathWrite))
    outputStream.writeBytes("xxx")
    rdd.map(_.toString).collect().foreach(line => outputStream.writeBytes(s"\n$line"))
    outputStream.close()
  }
}
