package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Text

import scala.beans.BeanProperty

class FastqRead extends FastaRead {
  @BeanProperty var quality: Text = new Text()

  // recreates fastq read (four lines) with object's fields
  override def toString: String = {
    s"${super.toString}\n+\n${quality}"
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other) || !other.isInstanceOf[FastqRead]) return false

    val otherFastqRead = other.asInstanceOf[FastqRead]
    if (quality != otherFastqRead.quality)
      return false

    true
  }

  override def hashCode(): Int = {
    var result = super.hashCode()
    result = HASH_CONST * result + quality.hashCode()

    result
  }

  override def readFields(in: DataInput): Unit = {
    clear()

    super.readFields(in)
    quality.readFields(in)
  }

  override def clear(): Unit = {
    super.clear()
    quality.clear()
  }

  override def write(out: DataOutput): Unit = {
    super.write(out)
    quality.write(out)
  }
}
