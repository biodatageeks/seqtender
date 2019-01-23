package org.biodatageeks
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{BDGVCFPipedRDD, RDD}
import htsjdk.variant.variantcontext.VariantContext
import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.VariantContextWritable

import scala.collection.Map

class CustomFunctions(rdd:RDD[Text]) {


  def pipeVCF(command: String, env: Map[String, String] = Map.empty) = new BDGVCFPipedRDD(rdd,BDGVCFPipedRDD.tokenize(command),env,
    null,
    null,
    true,
    encoding = "UTF-8",
    bufferSize = 8192)

}

object CustomFunctions {

  implicit def addCustomFunctions(rdd: RDD[Text]) = new CustomFunctions(rdd)
}