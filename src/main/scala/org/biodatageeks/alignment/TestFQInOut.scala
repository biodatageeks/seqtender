package org.biodatageeks.alignment

import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

object TestFQInOut {

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

    /*alignment.map { text => text.toString }.collect()
      .foreach { line => println(line) }*/

  }
}
