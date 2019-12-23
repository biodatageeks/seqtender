package org.biodatageeks.shared

import scala.collection.mutable.ArrayBuffer

object Tools {
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    buf += "/bin/sh"
    buf += "-c"
    buf += command
    buf
  }
}
