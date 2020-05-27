package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable, WritableUtils}

// this class represents single FASTQ read - its name, sequence and quality
@Deprecated
class FastqRead extends Writable {
  var sequence: Text = new Text()
  var quality: Text = new Text()
  var name: String = _

  def setName(n: String) {
    if (n == null)
      throw new IllegalArgumentException("Name cannot be null")

    name = n
  }

  def setSequence(seq: Text) {
    if (seq == null)
      throw new IllegalArgumentException("Sequence cannot be null")

    sequence = seq
  }

  def setQuality(qual: Text) {
    if (qual == null)
      throw new IllegalArgumentException("Quality cannot be null")

    quality = qual
  }

  def getName: String = { name }
  def getSequence: Text = { sequence }
  def getQuality: Text = { quality }

  def toText: Text = {
    new Text(toString)
  }

  // recreates fastq read (four lines) with object's fields
  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s"${name}")
    stringBuilder.append("\n")
    stringBuilder.append(sequence)
    stringBuilder.append("\n")
    stringBuilder.append("+")
    stringBuilder.append("\n")
    stringBuilder.append(quality)

    stringBuilder.toString()
  }

  override def equals(other: Any): Boolean = {
    if (other != null && other.isInstanceOf[FastqRead]) {
      val otherFastqRead = other.asInstanceOf[FastqRead]

      if (name == null && otherFastqRead.name != null || name != null && !(name == otherFastqRead.name))
        return false

      // sequence and quality can't be null
      if (sequence != otherFastqRead.sequence || quality != otherFastqRead.quality)
        return false

      return true
    }
    false
  }

  override def hashCode(): Int = {
    var result = sequence.hashCode();
    result = 37 * result + quality.hashCode()
    result = 37 * result + (if (name.nonEmpty && name != null) name.hashCode() else 0)

    result
  }

  override def readFields(in: DataInput): Unit = {
    clear()

    name = WritableUtils.readString(in)
    sequence.readFields(in)
    quality.readFields(in)
  }

  def clear(): Unit = {
    sequence.clear()
    quality.clear()
    name = null
  }

  override def write(out: DataOutput): Unit = {
    WritableUtils.writeString(out, s"${name}")
    sequence.write(out)
    WritableUtils.writeString(out, "+")
    quality.write(out)
  }
}
