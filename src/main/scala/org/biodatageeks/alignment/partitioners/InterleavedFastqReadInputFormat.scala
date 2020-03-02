package org.biodatageeks.alignment.partitioners

import java.io.{EOFException, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.util.LineReader

import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

// A class to split interleaved fastq file to smaller interleaved fastq files
// It's founded on the org.seqdoop.hadoop_bam.FastqInputFormat
class InterleavedFastqReadInputFormat extends FileInputFormat[Text, InterleavedFastqRead] {

  class InterleavedFastqReadRecordReader(val conf: Configuration, val split: FileSplit) extends RecordReader[Text, InterleavedFastqRead] {
    private var start: Long = split.getStart
    private var end: Long = start + split.getLength
    private var pos: Long = Long.MinValue

    private val file: Path = split.getPath
    private var lineReader: LineReader = null
    private var inputStream: InputStream = null

    private val currentKey: Text = new Text
    private val currentValue: InterleavedFastqRead = new InterleavedFastqRead

    private final val MAX_LINE_LENGTH = 20000 // or more?
    private final val sequencePattern: Regex = "[A-Za-z]+".r
    private final val qualityPattern: Regex = "[!-~]+".r


    val fs: FileSystem = file.getFileSystem(conf)
    val fileIn: FSDataInputStream = fs.open(file)

    val codecFactory = new CompressionCodecFactory(conf)
    val codec: CompressionCodec = codecFactory.getCodec(file)

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      if (codec == null) { // no codec.  Uncompressed file.
        setPositionAtFirstRecord(fileIn)
        inputStream = fileIn
      } else { // compressed file
        if (start != 0) throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ] Start position for compressed file is not 0! (found ${start})")
        inputStream = codec.createInputStream(fileIn)
        end = Long.MaxValue // read until the end of the file
      }

      lineReader = new LineReader(inputStream)
    }

    // Position the input stream at the start of the first record.
    private def setPositionAtFirstRecord(stream: FSDataInputStream): Unit = {
      if (start > 0) {
        // Advance to the start of the first record
        // We use a temporary LineReader to read lines until we find the
        // position of the right one.  We then seek the file to that position.
        stream.seek(start)
        val reader = new LineReader(stream)
        var bytesRead = 0

        breakable {
          do {
            val buffer: Text = new Text
            bytesRead = reader.readLine(buffer, Math.min(MAX_LINE_LENGTH, end - start).toInt)
            val bufferSequenceString = buffer.toString
            if (bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == '@' && bufferSequenceString.splitAt(bufferSequenceString.lastIndexOf('/'))._2 == "/1") break
            else start += bytesRead // line starts with @.
          } while (bytesRead > 0)
        }

        stream.seek(start)
      }
      pos = start
    }

    override def nextKeyValue(): Boolean = {
      if (pos >= end) return false // past end of slice
      try {
        isInterleavedFastqReadRead(currentKey, currentValue)
      } catch {
        case e: EOFException =>
          throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Unexpected end of file in interleaved fastq record at ${file.toString}: ${pos}. Read key: ${currentKey.toString}")
      }
    }

    protected def isInterleavedFastqReadRead(key: Text, value: InterleavedFastqRead): Boolean = {
      value.clear()
      isFastqReadRead(value.getFirstRead, 1)
      isFastqReadRead(value.getSecondRead, 2)

      if (value.getFirstRead.getName == value.getSecondRead.getName) key.set(new Text(value.getFirstRead.getName))
      else throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Names of two sequences aren't equal. File ${file.toString}: ${pos}. First read name: ${value.getFirstRead.getName}")

      true
    }

    private def isFastqReadRead(value: FastqRead, ordinalNumber: Int): Boolean = {
      val key: Text = new Text()
      val buffer: Text = new Text()

      // key - sequence name
      readLineInto(buffer)
      if(buffer.charAt(0) == '@') key.set(buffer)
      else throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Unexpected character in name in interleaved fastq record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      if(!key.toString.contains(s"/${ordinalNumber}"))
        throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Read name in interleaved fastq record isn't correct. It hasn't proper ordinal number. File ${file.toString}: ${pos}. Read key: ${key.toString}")

      value.setName(key.toString.substring(1, key.toString.indexOf('/')))

      // sequence
      readLineInto(buffer)
      if (sequencePattern.pattern.matcher(buffer.toString).matches()) value.setSequence(new Text(buffer))
      else throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Unexpected character in sequence in interleaved fastq record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      // separator
      readLineInto(buffer)
      if(buffer.getLength == 0 || buffer.charAt(0) != '+')
        throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Unexpected character in interleaved fastq record at ${file.toString}: ${pos}. Read key: ${key.toString}. This should be '+' separator.")

      // quality
      readLineInto(buffer)
      if (qualityPattern.pattern.matcher(buffer.toString).matches()) value.setQuality(buffer)
      else throw new RuntimeException(s"[SPLIT INTERLEAVED FASTQ]: Unexpected character in quality in interleaved fastq record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      true
    }

    private def readLineInto(dest: Text): Unit = {
      val bytesRead = lineReader.readLine(dest, MAX_LINE_LENGTH)
      if (bytesRead <= 0) throw new EOFException
      pos += bytesRead
    }

    override def getCurrentKey: Text = {
      currentKey
    }

    override def getCurrentValue: InterleavedFastqRead = {
      currentValue
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

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, InterleavedFastqRead] = {
    context.setStatus(split.toString)
    new InterleavedFastqReadRecordReader(context.getConfiguration, split.asInstanceOf[FileSplit])
  }
}
