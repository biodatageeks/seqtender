package org.biodatageeks.utils

import scala.collection.mutable.ArrayBuffer

object Tokenizer {
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    buf += "/bin/sh"
    buf += "-c"
    buf += command
    buf
  }
}
