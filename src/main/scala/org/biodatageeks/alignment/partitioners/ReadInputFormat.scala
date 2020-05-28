package org.biodatageeks.alignment.partitioners

import java.io.{EOFException, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader

abstract class ReadInputFormat[T] extends FileInputFormat[Text, T] {

  abstract class ReadRecordReader[X](val conf: Configuration, val split: FileSplit) extends RecordReader[Text, X] {
    private var start: Long = split.getStart
    private var end: Long = start + split.getLength
    protected var pos: Long = Long.MinValue

    protected val file: Path = split.getPath
    protected var lineReader: LineReader = null
    private var inputStream: InputStream = null

    private val currentKey: Text = new Text
    protected var currentValue: X = _

    val fs: FileSystem = file.getFileSystem(conf)
    val fileIn: FSDataInputStream = fs.open(file)

    val codecFactory = new CompressionCodecFactory(conf)
    val codec: CompressionCodec = codecFactory.getCodec(file)

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      if (codec == null) { // no codec.  Uncompressed file.
        setPositionAtFirstRecord(fileIn)
        inputStream = fileIn
      } else { // compressed file
        if (start != 0) throw new RuntimeException(s"[READER]: Start position for compressed file is not 0! (found ${start})")
        inputStream = codec.createInputStream(fileIn)
        end = Long.MaxValue // read until the end of the file
      }

      lineReader = new LineReader(inputStream)
    }

    // Position the input stream at the start of the first record.
    private def setPositionAtFirstRecord(stream: FSDataInputStream): Unit = {
      if (start > 0) {
        stream.seek(start)
        findFirstRecord(stream)
        stream.seek(start)
      }
      pos = start
    }

    private def findFirstRecord(stream: FSDataInputStream): Unit = {
      // LineReader is temporarily used to read lines until position of the first record will be found.
      // Then reading a file will be moved to that position.
      val reader = new LineReader(stream)
      var bytesRead = 0
      do {
        val buffer: Text = new Text
        bytesRead = reader.readLine(buffer, Math.min(ReadReader.MAX_LINE_LENGTH, end - start).toInt)
        if (isFirstRecordNameLine(bytesRead, buffer)) return
        else start += bytesRead
      } while (bytesRead > 0)
    }

    protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean

    protected def setValidRead(key: Text, value: X): Unit

    protected def readLineInto(destination: Text): Unit = {
      val bytesRead = lineReader.readLine(destination, ReadReader.MAX_LINE_LENGTH)
      if (bytesRead <= 0) throw new EOFException
      pos += bytesRead
    }

    override def nextKeyValue(): Boolean = {
      if (pos >= end) return false
      try {
        setValidRead(currentKey, currentValue)
        true
      } catch {
        case e: EOFException =>
          throw new RuntimeException(s"[READER]: Unexpected end of file ${file.toString} at ${pos}. Read key: ${currentKey.toString}")
      }
    }

    override def getCurrentKey: Text = {
      currentKey
    }

    override def getProgress: Float = {
      if (start == end) 1.0f
      else Math.min(1.0f, (pos - start) / (end - start).toFloat)
    }

    override def close(): Unit = {
      inputStream.close()
    }
  }

  override def isSplitable(context: JobContext, path: Path): Boolean = {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(path)
    codec == null
  }
}
