package org.biodatageeks.alignment.partitioners

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable}

import scala.beans.BeanProperty

class FastaRead extends Writable {
  protected val HASH_CONST = 37

  @BeanProperty var sequence: Text = new Text()
  @BeanProperty var name: Text = new Text()

  def toText: Text = {
    new Text(toString)
  }

  // recreates fasta read (dual line) with object's fields
  override def toString: String = {
    s"${name}\n${sequence}"
  }

  override def equals(other: Any): Boolean = {
    if (other == null || !other.isInstanceOf[FastaRead]) return false

    val otherFastqRead = other.asInstanceOf[FastaRead]
    if (name != otherFastqRead.name || sequence != otherFastqRead.sequence)
      return false

    true
  }

  override def hashCode(): Int = {
    var result = sequence.hashCode()
    result = HASH_CONST * result + name.hashCode()

    result
  }

  override def readFields(in: DataInput): Unit = {
    clear()

    name.readFields(in)
    sequence.readFields(in)
  }

  def clear(): Unit = {
    sequence.clear()
    name.clear()
  }

  override def write(out: DataOutput): Unit = {
    name.write(out)
    sequence.write(out)
  }
}
