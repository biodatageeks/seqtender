package org.biodatageeks

import org.biodatageeks.annotation.AnnotationFlow
import org.biodatageeks.conf.JobConfiguration
import scala.util.Success

object AnnotationJob {
  def main(args: Array[String]): Unit = {
    args match {
      case Array(promGateway, vcfPath, cacheDirPath, vepVersion, command, varAnnoPath) =>
        JobConfiguration.parseAnnotation(args.mkString(",")) match {
          case Success(config) => AnnotationFlow.calculateAnnotations(config)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"""
             |Wrong input arguments.
             |""".stripMargin
        )
    }
  }
}