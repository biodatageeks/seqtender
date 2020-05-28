package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class FastqReadInputFormat extends ReadInputFormat[FastqRead] {

  class FastqReadRecordReader(override val conf: Configuration, override val split: FileSplit)
    extends ReadRecordReader[FastqRead](conf, split) {

    currentValue = new FastqRead

    override protected def setValidRead(key: Text, value: FastqRead): Unit = {
      pos += ReadReader.setFastqReadAndReturnReadBytes(lineReader, key, value, file.toString)
    }

    override protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean = {
      bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == ReadReader.FASTQ_NAME_PREFIX_CHAR
    }

    override def getCurrentValue: FastqRead = {
      currentValue
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, FastqRead] = {
    context.setStatus(split.toString)
    new FastqReadRecordReader(context.getConfiguration, split.asInstanceOf[FileSplit])
  }
}
