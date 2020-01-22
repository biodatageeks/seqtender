package org.biodatageeks.shared

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{AlignmentPipedRDD, RDD, VCFPipedRDD}

import scala.collection.Map

class CustomRDDTextFunctions(rdd: RDD[Text]) {

  def pipeVCF(command: String, env: Map[String, String] = Map.empty) =
    new VCFPipedRDD(
      rdd,
      Tools.tokenize(command),
      env,
      null,
      null,
      true,
      encoding = "UTF-8",
      bufferSize = 8192)

  def pipeRead(command: String, env: Map[String, String] = Map.empty) =
    new AlignmentPipedRDD(
      rdd,
      Tools.tokenize(command),
      env,
      null,
      null,
      true,
      encoding = "UTF-8",
      bufferSize = 8192)

}

object CustomRDDTextFunctions {
  implicit def addCustomFunctions(rdd: RDD[Text]) = new CustomRDDTextFunctions(rdd)
}
