package org.biodatageeks.shared

case class IllegalFileExtensionException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
