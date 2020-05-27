package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.{Text, Writable}

// this class represents single interleaved FASTQ read - its two reads with name, sequence and quality
@Deprecated
class InterleavedFastqRead extends Writable {
  protected var firstRead: FastqRead = new FastqRead()
  protected var secondRead: FastqRead = new FastqRead()

  def setFirstRead(fR: FastqRead) {
    if (fR == null)
      throw new IllegalArgumentException("First read cannot be null")

    firstRead = fR
  }

  def setSecondRead(sR: FastqRead) {
    if (sR == null)
      throw new IllegalArgumentException("Second read cannot be null")

    secondRead = sR
  }

  def getFirstRead: FastqRead = { firstRead }
  def getSecondRead: FastqRead = { secondRead }

  def toText: Text = {
    new Text(toString)
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(readToString(firstRead))
    stringBuilder.append("\n")
    stringBuilder.append(readToString(secondRead))

    stringBuilder.toString()
  }

  private def readToString(read: FastqRead): String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s"${read.getName}")
    stringBuilder.append("\n")
    stringBuilder.append(read.getSequence)
    stringBuilder.append("\n")
    stringBuilder.append("+")
    stringBuilder.append("\n")
    stringBuilder.append(read.getQuality)

    stringBuilder.toString()
  }

  override def equals(other: Any): Boolean = {
    if (other != null && other.isInstanceOf[InterleavedFastqRead]) {
      val otherInterleavedFastqRead = other.asInstanceOf[InterleavedFastqRead]

      return (firstRead.equals(otherInterleavedFastqRead.firstRead) && secondRead.equals(otherInterleavedFastqRead.secondRead)) ||
        (firstRead.equals(otherInterleavedFastqRead.secondRead) && secondRead.equals(otherInterleavedFastqRead.firstRead))

    }
    false
  }

  override def hashCode(): Int = {
    var result = firstRead.hashCode()

    result = 37 * secondRead.sequence.hashCode();
    result = 37 * result + secondRead.quality.hashCode()
    result = 37 * result + (if (secondRead.name.nonEmpty && secondRead.name != null) secondRead.name.hashCode() else 0)

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

