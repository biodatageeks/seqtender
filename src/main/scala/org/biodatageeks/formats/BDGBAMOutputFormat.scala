package org.biodatageeks.formats

import htsjdk.samtools.SAMFileHeader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.{KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, SAMRecordWritable}
import java.io.ByteArrayInputStream
import java.nio.charset.Charset

import org.biodatageeks.conf.InternalParams


class BDGBAMOutputFormat[K,V] extends KeyIgnoringBAMOutputFormat[K] with Serializable {
  setWriteHeader(true)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, SAMRecordWritable] = {
    val conf = context.getConfiguration

    val outPath = new Path(conf.get(InternalParams.PATH))
    val headerString = conf.get(InternalParams.HEADER_STRING)
    val inputStream = new ByteArrayInputStream(headerString.getBytes(Charset.forName("UTF-8")))
    readSAMHeaderFrom(inputStream, conf)
    new KeyIgnoringBAMRecordWriter[K](outPath,header, true, context)

  }
}