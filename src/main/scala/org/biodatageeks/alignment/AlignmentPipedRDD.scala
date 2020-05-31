package org.apache.spark.rdd

import htsjdk.samtools.{SAMRecord, SamInputResource, SamReaderFactory, ValidationStringency}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.reflect.ClassTag

class AlignmentPipedRDD[T: ClassTag](prev: RDD[T],
                                     command: Seq[String],
                                     envVars: Map[String, String] = Map.empty,
                                     printPipeContext: (String => Unit) => Unit,
                                     printRDDElement: (T, String => Unit) => Unit,
                                     separateWorkingDir: Boolean,
                                     bufferSize: Int,
                                     encoding: String)
  extends PipedRDDBDG[SAMRecord, T](prev, command, envVars, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding) {

  override def compute(split: Partition, context: TaskContext): Iterator[SAMRecord] = {
    val processDetails = runProcess(split, context)

    // Return an iterator that read lines from the process's stdout
    val inputStream = processDetails.process.getInputStream
    val samInputResource = SamInputResource.of(inputStream)

    val factory = SamReaderFactory.makeDefault()
      .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS, SamReaderFactory.Option.VALIDATE_CRC_CHECKSUMS)
      .validationStringency(ValidationStringency.SILENT);

    val samReader = factory.open(samInputResource)
    val samIterator = samReader.iterator().asScala

    new Iterator[SAMRecord] {
      def next(): SAMRecord = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        samIterator.next()
      }

      def hasNext(): Boolean = {
        val result = if (samIterator.hasNext) {
          true
        } else {
          val exitStatus = processDetails.process.waitFor()
          cleanupTaskDirectory(processDetails)
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        propagateChildException(processDetails)
        result
      }
    }
  }
}
