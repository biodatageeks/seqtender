package org.biodatageeks.alignment

import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

object TestFQInOut {
  val pathWrite = "/home/patrycja/Pulpit/Praca_inzynierska/00_Seqtender/bdg-seqtender/data/int.txt"

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
    alignment.saveAsTextFile(pathWrite)

  }
}
