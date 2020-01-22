package org.apache.spark.rdd

import java.io._
import java.util.concurrent.atomic.AtomicReference

import htsjdk.samtools.{SAMRecord, SamInputResource, SamReaderFactory, ValidationStringency}
import org.apache.spark.util.Utils
import org.apache.spark.{Partition, SparkEnv, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.io.Source
import scala.reflect.ClassTag

class AlignmentPipedRDD[T: ClassTag](
                                 prev: RDD[T],
                                 command: Seq[String],
                                 envVars: Map[String, String] = Map.empty,
                                 printPipeContext: (String => Unit) => Unit,
                                 printRDDElement: (T, String => Unit) => Unit,
                                 separateWorkingDir: Boolean,
                                 bufferSize: Int,
                                 encoding: String)
  extends RDD[SAMRecord](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
    * A FilenameFilter that accepts anything that isn't equal to the name passed in.
    *
    * @param filterName of file or directory to leave out
    */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[SAMRecord] = {
    val processBuilder = new ProcessBuilder(command.asJava)
    // Add the environmental variables to the process.
    val currentEnvVars = processBuilder.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    if (split.isInstanceOf[HadoopPartition]) {
      val hadoopSplit = split.asInstanceOf[HadoopPartition]
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
    }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    logDebug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      logDebug("currentDir = " + currentDir.getAbsolutePath)
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")
        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        for (file <- currentDir.list(tasksDirFilter)) {
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath),
            new File(taskDirectory + File.separator + fileWithDir.getName))
        }
        processBuilder.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception => logError("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e)
      }
    }

    val process = processBuilder.start()
    val env = SparkEnv.get
    val childThreadException = new AtomicReference[Throwable](null)

    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $command") {
      override def run(): Unit = {
        val err = process.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(encoding).getLines) {
            // scalastyle:off println
            System.err.println(line)
            // scalastyle:on println
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread(s"stdin writer for $command") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        val out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(process.getOutputStream, encoding), bufferSize))
        try {
          // scalastyle:off println
          // input the pipe context firstly
          if (printPipeContext != null) {
            printPipeContext(out.println)
          }
          for (elem <- firstParent[T].iterator(split, context)) {
            if (printRDDElement != null) {
              printRDDElement(elem, out.println)
            } else {
              out.println(elem)
            }
          }
          // scalastyle:on println
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }.start()

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
          cleanup()
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        propagateChildException()
        result
      }

      private def cleanup(): Unit = {
        // cleanup task working directory if used
        if (workInTaskDirectory) {
          scala.util.control.Exception.ignoring(classOf[IOException]) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
          logDebug(s"Removed task working directory $taskDirectory")
        }
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          val commandRan = command.mkString(" ")
          logError(s"Caught exception while running pipe() operator. Command ran: $commandRan. " +
            s"Exception: ${t.getMessage}")
          process.destroy()
          cleanup()
          throw t
        }
      }
    }
  }
}
