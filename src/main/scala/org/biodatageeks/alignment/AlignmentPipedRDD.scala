package org.apache.spark.rdd

import java.io._
import java.util.concurrent.atomic.AtomicReference

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
  extends PipedRDDBDG[SAMRecord, T](prev = prev, command = command, envVars = envVars, printPipeContext = printPipeContext, printRDDElement = printRDDElement,
    separateWorkingDir = separateWorkingDir, bufferSize = bufferSize, encoding = encoding) {

  override def compute(split: Partition, context: TaskContext): Iterator[SAMRecord] = {
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString

    val processBuilderAndTaskDirectory = setUpProcessAndTaskDirectory(split, taskDirectory)
    val workInTaskDirectory = processBuilderAndTaskDirectory._2
    val process = processBuilderAndTaskDirectory._1.start()

    val childThreadException = new AtomicReference[Throwable](null)

    startStdErrReaderThread(process, childThreadException)
    startWriterThread(split, process, context, childThreadException)

    // Return an iterator that read lines from the process's stdout
    val inputStream = process.getInputStream
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
          val exitStatus = process.waitFor()
          cleanup(workInTaskDirectory, taskDirectory)
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        propagateChildException(process, childThreadException, workInTaskDirectory, taskDirectory)
        result
      }
    }
  }
}
