package org.biodatageeks.annotation

import org.apache.spark.sql.SparkSession
import org.biodatageeks.conf.AnnotationConfiguration

object AnnotationFlow {
  val FlowName = "annotation-job"

  def calculateAnnotations(config: AnnotationConfiguration): Unit = {
    val spark = createSparkSession()

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    stageMetrics.begin()

    val annotated = SeqTenderVCF.pipeVCF(config.vcfPath, config.command, spark)
    val variants = CustomVariantContextFunctions.addCustomFunctions(annotated)
    variants.saveDISQAsVCFFile(config.varAnnoPath)(spark)

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
