package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Text

import scala.beans.BeanProperty

class InterleavedFastqRead extends WritableText {
  @BeanProperty var firstRead: FastqRead = new FastqRead()
  @BeanProperty var secondRead: FastqRead = new FastqRead()

  def toText: Text = { new Text(toString) }

  override def toString: String = { s"${firstRead.toString}\n${secondRead.toString}" }

  override def equals(other: Any): Boolean = {
    if (other == null || !other.isInstanceOf[InterleavedFastqRead]) return false

    val otherInterleavedFastqRead = other.asInstanceOf[InterleavedFastqRead]

    (firstRead.equals(otherInterleavedFastqRead.firstRead) && secondRead.equals(otherInterleavedFastqRead.secondRead)) ||
      (firstRead.equals(otherInterleavedFastqRead.secondRead) && secondRead.equals(otherInterleavedFastqRead.firstRead))
  }

  override def hashCode(): Int = {
    var result = firstRead.hashCode()

    result = HASH_CONST * result + secondRead.getName.hashCode()
    result = HASH_CONST * result + secondRead.getSequence.hashCode()
    result = HASH_CONST * result + secondRead.getQuality.hashCode()

    result
  }

  override def readFields(in: DataInput): Unit = {
    firstRead.readFields(in)
    secondRead.readFields(in)
  }

  def clear(): Unit = {
    firstRead.clear()
    secondRead.clear()
  }

  override def write(out: DataOutput): Unit = {
    firstRead.write(out)
    secondRead.write(out)
  }
}
