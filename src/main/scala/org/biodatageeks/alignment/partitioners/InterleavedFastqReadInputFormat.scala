package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.util.matching.Regex

class InterleavedFastqReadInputFormat extends ReadInputFormat[InterleavedFastqRead] {

  class InterleavedFastqReadRecordReader(override val conf: Configuration, override val split: FileSplit) extends ReadRecordReader[InterleavedFastqRead](conf, split) {
    override protected val EXCEPTION_INFO_PREFIX = "SPLIT INTERLEAVED FASTQ"
    override protected val EXCEPTION_FORMAT_INFO = "interleaved fastq"
    private final val NAME_PREFIX_CHAR = '@'
    private final val firstReadRegex: Regex = ".+([/ +_]1| 1:[YN]:[02468]+:[0-9ACTNG]+)$".r
    currentValue = new InterleavedFastqRead

    override protected def isCorrectRead(key: Text, value: InterleavedFastqRead): Boolean = {
      value.clear()
      isFastqReadCorrect(value.getFirstRead)
      isFastqReadCorrect(value.getSecondRead)
    }

    private def isFastqReadCorrect(value: FastqRead): Boolean = {
      value.clear()
      val key: Text = new Text()
      val buffer: Text = new Text()

      // key - sequence name
      readLineInto(buffer)
      if (buffer.toString.charAt(0) == NAME_PREFIX_CHAR) key.set(buffer)
      else throw new RuntimeException(s"[${EXCEPTION_INFO_PREFIX}]: Unexpected character in name in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      value.setName(key)

      // sequence
      readLineInto(buffer)
      if (sequencePattern.pattern.matcher(buffer.toString).matches()) value.setSequence(new Text(buffer))
      else throw new RuntimeException(s"[$EXCEPTION_INFO_PREFIX]: Unexpected character in sequence in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      // separator
      readLineInto(buffer)
      if(buffer.getLength == 0 || buffer.charAt(0) != SEPARATOR_CHAR)
        throw new RuntimeException(s"[$EXCEPTION_INFO_PREFIX]: Unexpected character in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}. This should be '$SEPARATOR_CHAR' separator.")

      // quality
      readLineInto(buffer)
      if (qualityPattern.pattern.matcher(buffer.toString).matches()) value.setQuality(buffer)
      else throw new RuntimeException(s"[$EXCEPTION_INFO_PREFIX]: Unexpected character in quality in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      true
    }

    override protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean = {
      bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == '@' && firstReadRegex.pattern.matcher(buffer.toString).matches()
    }

    override def getCurrentValue: InterleavedFastqRead = {
      currentValue
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, InterleavedFastqRead] = {
    context.setStatus(split.toString)
    new InterleavedFastqReadRecordReader(context.getConfiguration, split.asInstanceOf[FileSplit])
  }
}
