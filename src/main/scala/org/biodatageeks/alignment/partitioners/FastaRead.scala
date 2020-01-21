package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable, WritableUtils}

// this class represents single FASTA read - its name and sequence
class FastaRead extends Writable {
  protected var sequence: Text = new Text()
  protected var name: String = _


  def setName(n: String) {
    if (n == null)
      throw new IllegalArgumentException("Name cannot be null")

    name = n
  }

  def setSequence(seq: Text) {
    if (seq == null)
      throw new IllegalArgumentException("Name cannot be null")

    sequence = seq
  }

  def getName: String = { name }
  def getSequence: Text = { sequence }

  def toText: Text = {
    new Text(toString)
  }

  // recreates fasta read (dual line) with object's fields
  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s">${name}")
    stringBuilder.append("\n")
    stringBuilder.append(sequence)

    stringBuilder.toString()
  }

  override def equals(other: Any): Boolean = {
    if (other != null && other.isInstanceOf[FastaRead]) {
      val otherFastaRead = other.asInstanceOf[FastaRead]

      if (name == null && otherFastaRead.name != null || name != null && !(name == otherFastaRead.name))
        return false

      // sequence can't be null
      if (!(sequence == otherFastaRead.sequence))
        return false

      return true
    }
    false
  }

  override def hashCode(): Int = {
    var result = sequence.hashCode();
    result = 37 * result + (if (name.nonEmpty && name != null) name.hashCode() else 0)

    result
  }

  override def readFields(in: DataInput): Unit = {
    clear()

    name = WritableUtils.readString(in)
    sequence.readFields(in)
  }

  def clear(): Unit = {
    sequence.clear()
    name = null
  }

  override def write(out: DataOutput): Unit = {
    WritableUtils.writeString(out, s">${name}")
    sequence.write(out)
  }
}
