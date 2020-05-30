package org.biodatageeks.utils

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.biodatageeks.alignment.AlignmentPipedRDDNew
import org.biodatageeks.annotation.VCFPipedRDDNew

import scala.collection.Map

class CustomRDDTextFunctions(rdd: RDD[Text]) {

  def pipeVCF(command: String, env: Map[String, String] = Map.empty) =
    new VCFPipedRDDNew(
      rdd,
      Tokenizer.tokenize(command),
      env,
      null,
      null,
      true,
      encoding = "UTF-8",
      bufferSize = 8192)

  def pipeRead(command: String, env: Map[String, String] = Map.empty) =
    new AlignmentPipedRDDNew(
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
