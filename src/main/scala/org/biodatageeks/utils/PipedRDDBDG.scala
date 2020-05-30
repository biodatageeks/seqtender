package org.apache.spark.rdd

import java.io._
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.util.Utils
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.io.Source
import scala.reflect.ClassTag

abstract class PipedRDDBDG[X: ClassTag, T: ClassTag](prev: RDD[T],
                                                     command: Seq[String],
                                                     envVars: Map[String, String] = Map.empty,
                                                     printPipeContext: (String => Unit) => Unit,
                                                     printRDDElement: (T, String => Unit) => Unit,
                                                     separateWorkingDir: Boolean,
                                                     bufferSize: Int,
                                                     encoding: String) extends RDD[X](prev) {

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[X]

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

  protected def setUpProcessAndTaskDirectory(split: Partition, taskDirectory: String): (ProcessBuilder, Boolean) = {
    var workInTaskDirectory = false
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
    (processBuilder, workInTaskDirectory)
  }

  protected def startStdErrReaderThread(process: Process, childThreadException: AtomicReference[Throwable]): Unit = {
    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $command") {
      override def run(): Unit = {
        val err = process.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(encoding).getLines) {
            logError(line)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }.start()
  }

  protected def startWriterThread(split: Partition, process: Process, context: TaskContext, childThreadException: AtomicReference[Throwable]): Unit = {
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
  }

  protected def cleanup(workInTaskDirectory: Boolean, taskDirectory: String): Unit = {
    // cleanup task working directory if used
    if (workInTaskDirectory) {
      scala.util.control.Exception.ignoring(classOf[IOException]) {
        Utils.deleteRecursively(new File(taskDirectory))
      }
      logDebug(s"Removed task working directory $taskDirectory")
    }
  }

  protected def propagateChildException(process: Process, childThreadException: AtomicReference[Throwable],
                                        workInTaskDirectory: Boolean, taskDirectory: String): Unit = {
    val t = childThreadException.get()
    if (t != null) {
      val commandRan = command.mkString(" ")
      logError(s"Caught exception while running pipe() operator. Command ran: $commandRan. " +
        s"Exception: ${t.getMessage}")
      process.destroy()
      cleanup(workInTaskDirectory, taskDirectory)
      throw t
    }
  }
}

class ProcessDetails(processBuilder: ProcessBuilder,
                     workInTaskDirectory: Boolean,
                     taskDirectory: String,
                     childThreadException: AtomicReference[Throwable])
