package org.biodatageeks.alignment

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}


object SeqTenderAlignment {

  def pipeReads(readsPath: String, command: String, sparkSession: SparkSession): RDD[Text] = {
    sparkSession.sparkContext.hadoopConfiguration.setInt("mapred.max.split.size", 40000)

    println(sparkSession.sparkContext.hadoopConfiguration.get("mapred.max.split.size"))
    makeFQRdds(sparkSession, readsPath)
  }

  def makeFQRdds(spark: SparkSession, inputPath: String): RDD[Text] = {
    spark.sparkContext
      .newAPIHadoopFile(inputPath,
        classOf[FastqInputFormat],
        classOf[Text],
        classOf[SequencedFragment],
        spark.sparkContext.hadoopConfiguration)
      .asInstanceOf[NewHadoopRDD[Text, SequencedFragment]]
      .mapPartitionsWithInputSplit { (inputSplit, iterator) =>
        //val mappedIterator = iterator.map(_._2)
        var tempList = List[Text]()

        for(read <- iterator) {
          // sequence and quality
          tempList = new Text(read._2.getQuality) :: new Text("+") :: new Text(read._2.getSequence.toString) ::
            // read name
            new Text(s"@${read._1.toString}") :: tempList
        }

        /*mappedIterator.foreach(line => tempList = new Text(line.toString) :: tempList)*/

        tempList.reverseIterator
      }
  }
}
