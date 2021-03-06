package org.apache.spark.rdd

import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.{VCFCodec, VCFHeader}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.Map
import scala.reflect.ClassTag

class VariantContextWithHeader(val context: VariantContext, val header: VCFHeader) extends VariantContext(context) {
  def getHeader: VCFHeader = header

  def getVariantContext = context
}

class VariantsPipedRDD[T: ClassTag](prev: RDD[T],
                                    command: Seq[String],
                                    envVars: Map[String, String] = Map.empty,
                                    printPipeContext: (String => Unit) => Unit,
                                    printRDDElement: (T, String => Unit) => Unit,
                                    separateWorkingDir: Boolean,
                                    bufferSize: Int,
                                    encoding: String)
  extends BioPipedRDD[VariantContextWithHeader, T](prev, command, envVars, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding) {

  override def compute(split: Partition, context: TaskContext): Iterator[VariantContextWithHeader] = {
    val processDetails = runProcess(split, context)

    // Return an iterator that read lines from the process's stdout
    val inputStream = processDetails.process.getInputStream
    val lri = new AsciiLineReaderIterator(new AsciiLineReader(inputStream))
    val codec = new VCFCodec()

    // read the header
    val header = codec.readActualHeader(lri).asInstanceOf[VCFHeader]

    new Iterator[VariantContextWithHeader] {
      def next(): VariantContextWithHeader = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        new VariantContextWithHeader(codec.decode(lri.next()), header)

      }

      def hasNext(): Boolean = {
        val result = if (lri.hasNext) {
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
