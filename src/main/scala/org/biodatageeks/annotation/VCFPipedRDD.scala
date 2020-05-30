package org.apache.spark.rdd

import java.io._
import java.util.concurrent.atomic.AtomicReference

import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.{VCFCodec, VCFHeader}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.Map
import scala.reflect.ClassTag

class VariantContextWithHeaderBDG(val context: VariantContext, val header: VCFHeader) extends VariantContext(context) {
  def getHeader: VCFHeader = header

  def getVariantContext = context
}

class VCFPipedRDD[T: ClassTag](prev: RDD[T],
                               command: Seq[String],
                               envVars: Map[String, String] = Map.empty,
                               printPipeContext: (String => Unit) => Unit,
                               printRDDElement: (T, String => Unit) => Unit,
                               separateWorkingDir: Boolean,
                               bufferSize: Int,
                               encoding: String)
  extends PipedRDDBDG[VariantContextWithHeaderBDG, T](prev, command, envVars, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding) {

  override def compute(split: Partition, context: TaskContext): Iterator[VariantContextWithHeaderBDG] = {
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString

    val processBuilderAndIsTaskDirectoryValue = setUpProcessAndTaskDirectory(split, taskDirectory)
    val process = processBuilderAndIsTaskDirectoryValue._1.start()
    val workInTaskDirectory = processBuilderAndIsTaskDirectoryValue._2

    val childThreadException = new AtomicReference[Throwable](null)

    startStdErrReaderThread(process, childThreadException)
    startWriterThread(split, process, context, childThreadException)

    // Return an iterator that read lines from the process's stdout
    val inputStream = process.getInputStream
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(inputStream))
    val codec = new VCFCodec()

    // read the header
    val header = codec.readActualHeader(lri).asInstanceOf[VCFHeader]

    new Iterator[VariantContextWithHeaderBDG] {
      def next(): VariantContextWithHeaderBDG = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        new VariantContextWithHeaderBDG(codec.decode(lri.next()), header)

      }

      def hasNext(): Boolean = {
        val result = if (lri.hasNext) {
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
