package org.biodatageeks.alignment.partitioners

case class ReaderException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(s"[READER]: $message", cause)
