package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class FastaReadInputFormat extends ReadInputFormat[FastaRead] {

  class FastaReadRecordReader(override val conf: Configuration, override val split: FileSplit)
    extends ReadRecordReader[FastaRead](conf, split) {

    currentValue = new FastaRead

    override protected def setValidRead(key: Text, value: FastaRead): Unit = {
      pos += ReadReader.setFastaReadAndReturnReadBytes(lineReader, key, value, file.toString)
    }

    override protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean = {
      bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == ReadReader.FASTA_NAME_PREFIX_CHAR
    }

    override def getCurrentValue: FastaRead = {
      currentValue
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, FastaRead] = {
    context.setStatus(split.toString)
    new FastaReadRecordReader(context.getConfiguration, split.asInstanceOf[FileSplit])
  }
}
