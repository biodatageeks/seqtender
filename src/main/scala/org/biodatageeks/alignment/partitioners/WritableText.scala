package org.biodatageeks.alignment.partitioners

import org.apache.hadoop.io.{Text, Writable}

trait WritableText extends Writable {
  protected val HASH_CONST = 37

  def toText: Text
}
