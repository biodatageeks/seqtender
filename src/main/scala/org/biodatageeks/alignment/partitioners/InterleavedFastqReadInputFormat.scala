package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.util.matching.Regex

class InterleavedFastqReadInputFormat extends ReadInputFormat[InterleavedFastqRead] {

  class InterleavedFastqReadRecordReader(override val conf: Configuration, override val split: FileSplit)
    extends ReadRecordReader[InterleavedFastqRead](conf, split) {

    private final val firstReadRegex: Regex = ".+([/ +_]1| 1:[YN]:[02468]+:[0-9ACTNG]+)$".r
    currentValue = new InterleavedFastqRead

    override protected def setValidRead(key: Text, value: InterleavedFastqRead): Unit = {
      value.clear()
      pos += ReadReader.setFastqReadAndReturnReadBytes(lineReader, key, value.getFirstRead, file.toString)
      pos += ReadReader.setFastqReadAndReturnReadBytes(lineReader, key, value.getSecondRead, file.toString)
    }

    override protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean = {
      bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == ReadReader.FASTQ_NAME_PREFIX_CHAR &&
        firstReadRegex.pattern.matcher(buffer.toString).matches()
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
