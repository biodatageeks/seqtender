package org.biodatageeks.utils

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{AlignmentsPipedRDD, RDD, VariantsPipedRDD}

import scala.collection.Map

class CustomRDDTextFunctions(rdd: RDD[Text]) {

  def pipeVCF(command: String, env: Map[String, String] = Map.empty) =
    new VariantsPipedRDD(
      rdd,
      Tokenizer.tokenize(command),
      env,
      null,
      null,
      true,
      encoding = "UTF-8",
      bufferSize = 8192)

  def pipeRead(command: String, env: Map[String, String] = Map.empty) =
    new AlignmentsPipedRDD(
      rdd,
      Tokenizer.tokenize(command),
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
