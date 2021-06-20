package org.biodatageeks

import org.biodatageeks.alignment.AlignmentFlow
import org.biodatageeks.conf.JobConfiguration
import scala.util.Success

object AlignmentJob {
  def main(args: Array[String]): Unit = {
    args match {
      case Array(promGateway, readsFilePath, refPath, command, bamFilePath) =>
        JobConfiguration.parseAlignment(args.mkString(",")) match {
          case Success(config) => AlignmentFlow.calculateAlignments(config)
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
