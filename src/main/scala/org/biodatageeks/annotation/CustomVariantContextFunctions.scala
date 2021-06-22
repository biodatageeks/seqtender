package org.biodatageeks.annotation

import org.apache.log4j.Logger
import org.apache.spark.rdd.{RDD, VariantContextWithHeader}
import org.apache.spark.sql.SparkSession
import org.disq_bio.disq.{HtsjdkVariantsRdd, HtsjdkVariantsRddStorage, VariantsFormatWriteOption}


class CustomVariantContextFunctions(rdd : RDD[VariantContextWithHeader]) extends Serializable {

  val logger = Logger.getLogger(getClass.getName)
  def saveAsVCFFile(path:String)(implicit sparkSession: SparkSession) = saveDISQAsVCFFile(path)

  def saveDISQAsVCFFile(path:String)(implicit sparkSession: SparkSession) = {
      //FIXME add support for multiple files

    val variants = rdd.map(_.getVariantContext)
    val header = rdd.first().getHeader
      val variantsRDD = new HtsjdkVariantsRdd(header, variants)
      HtsjdkVariantsRddStorage
      .makeDefault(sparkSession.sparkContext)
      .write(variantsRDD,
        path,
        VariantsFormatWriteOption.VCF_GZ
      )

  }
}

object CustomVariantContextFunctions {
  implicit def addCustomFunctions(rdd : RDD[VariantContextWithHeader]) = new CustomVariantContextFunctions(rdd)
}
