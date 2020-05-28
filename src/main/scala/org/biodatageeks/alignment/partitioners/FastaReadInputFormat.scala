package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class FastaReadInputFormat extends ReadInputFormat[FastaRead] {

  class FastaReadRecordReader(override val conf: Configuration, override val split: FileSplit) extends ReadRecordReader[FastaRead](conf, split) {
    override protected val EXCEPTION_INFO_PREFIX = "SPLIT FASTA"
    override protected val EXCEPTION_FORMAT_INFO = "fasta"
    private final val NAME_PREFIX_CHAR = '>'
    currentValue = new FastaRead

    override protected def isCorrectRead(key: Text, value: FastaRead): Boolean = {
      value.clear()
      val buffer: Text = new Text()

      // key - sequence name
      readLineInto(buffer)
      if (buffer.toString.charAt(0) == NAME_PREFIX_CHAR) key.set(buffer)
      else throw new RuntimeException(s"[${EXCEPTION_INFO_PREFIX}]: Unexpected character in name in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      value.setName(key)

      // sequence
      readLineInto(buffer)
      if (sequencePattern.pattern.matcher(buffer.toString).matches()) value.setSequence(buffer)
      else throw new RuntimeException(s"[$EXCEPTION_INFO_PREFIX]: Unexpected character in sequence in $EXCEPTION_FORMAT_INFO record at ${file.toString}: ${pos}. Read key: ${key.toString}")

      true
    }

    override protected def isFirstRecordNameLine(bytesRead: Int, buffer: Text): Boolean = {
      bytesRead > 0 && buffer.getLength > 0 && buffer.charAt(0) == NAME_PREFIX_CHAR
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
