package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}

class InterleavedFastqReadInputFormat extends FileInputFormat[Text, InterleavedFastqRead] {

  class InterleavedFastqReadRecordReader(val conf: Configuration, val split: FileSplit) extends RecordReader[Text, InterleavedFastqRead] {

  }
}
