package org.biodatageeks

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{RDD, VCFPipedRDD}

import scala.collection.Map

class CustomFunctions(rdd: RDD[Text]) {

  def pipeVCF(command: String, env: Map[String, String] = Map.empty) =
    new VCFPipedRDD(
      rdd,
      VCFPipedRDD.tokenize(command),
      env,
      null,
      null,
      true,
      encoding = "UTF-8",
      bufferSize = 8192)

}

object CustomFunctions {
  implicit def addCustomFunctions(rdd: RDD[Text]) = new CustomFunctions(rdd)
}