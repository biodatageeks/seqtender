package org.biodatageeks.conf

import scala.util.Try

object JobConfiguration {
  def parseAlignment(args: String): Try[AlignmentConfiguration] = {
    val Array(promGateway, readsFilePath, refPath, command, bamFilePath) = args.split(",")
    Try(AlignmentConfiguration(promGateway, readsFilePath, refPath, command, bamFilePath))
  }

  def parseAnnotation(args: String): Try[AnnotationConfiguration] = {
    val Array(promGateway, vcfPath, cacheDirPath, vepVersion, command, varAnnoPath) = args.split(",")
    Try(AnnotationConfiguration(promGateway, vcfPath, cacheDirPath, vepVersion, command, varAnnoPath))
  }
}

case class AlignmentConfiguration(promGateway: String, readsFilePath: String, refPath: String, command: String,
                            bamFilePath: String)

case class AnnotationConfiguration(promGateway: String, vcfPath: String, cacheDirPath: String, vepVersion: String,
                                   command: String, varAnnoPath: String)