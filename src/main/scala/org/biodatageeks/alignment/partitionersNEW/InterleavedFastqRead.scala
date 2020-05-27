package org.biodatageeks.alignment.partitionersNEW

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

import scala.beans.BeanProperty

class InterleavedFastqRead extends Writable {
  @BeanProperty var firstRead: FastqRead = new FastqRead()
  @BeanProperty var secondRead: FastqRead = new FastqRead()

  override def toString: String = {
    s"${firstRead.toString}\n${secondRead.toString}"
  }

  override def equals(other: Any): Boolean = {
    if (other == null || !other.isInstanceOf[InterleavedFastqRead]) return false

    val otherInterleavedFastqRead = other.asInstanceOf[InterleavedFastqRead]

    (firstRead.equals(otherInterleavedFastqRead.firstRead) && secondRead.equals(otherInterleavedFastqRead.secondRead)) ||
      (firstRead.equals(otherInterleavedFastqRead.secondRead) && secondRead.equals(otherInterleavedFastqRead.firstRead))
  }

  override def hashCode(): Int = {
    var result = firstRead.hashCode()

    result = 37 * result + secondRead.getName.hashCode()
    result = 37 * result + secondRead.getSequence.hashCode()
    result = 37 * result + secondRead.getQuality.hashCode()

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
