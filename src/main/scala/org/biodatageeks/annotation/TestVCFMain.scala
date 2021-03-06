package org.biodatageeks.annotation

import org.apache.spark.sql.SparkSession

object TestVCFMain {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      //        .config("spark.driver.memory","2g")
      .master("local[2]")
      .getOrCreate()

    val vc = SeqTenderVCF
      .pipeVCF(
        args(0),
        "docker run --rm -i biodatageeks/bdg-vt:latest vt decompose - ",
        ss)
    ss.time(println(vc.count()))
  }
}
