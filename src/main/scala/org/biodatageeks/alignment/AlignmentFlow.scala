package org.biodatageeks.alignment

import org.apache.spark.sql.SparkSession
import org.biodatageeks.conf.AlignmentConfiguration


object AlignmentFlow {
  val FlowName = "alignment-job"

  def calculateAlignments(config: AlignmentConfiguration): Unit = {
    val spark = createSparkSession()

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    stageMetrics.begin()

    val alignmentRDD = SeqTenderAlignment.pipeReads(config.readsFilePath, config.command)(spark)
    val reads = CustomRDDSAMRecordFunctions.addCustomFunctions(alignmentRDD)
    reads.saveAsBAMFile(config.bamFilePath)(spark)

    stageMetrics.end()
    stageMetrics.sendReportPrometheus(config.promGateway, FlowName, "job_name", spark.sparkContext.appName)

    //https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/issues/1194
    spark.sparkContext.stop()
  }

  private def createSparkSession(): SparkSession =
    SparkSession
      .builder()
      .config("org.biodatageeks.seqtender.bamIOLib", "disq")
      .getOrCreate()
}
