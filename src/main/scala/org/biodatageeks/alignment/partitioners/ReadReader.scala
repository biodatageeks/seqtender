package org.biodatageeks.alignment.partitioners

import java.io.EOFException

import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import scala.util.matching.Regex

object ReadReader {
  final val MAX_LINE_LENGTH = 20000
  final val FASTA_NAME_PREFIX_CHAR = '>'
  final val FASTQ_NAME_PREFIX_CHAR = '@'
  private final val SEPARATOR_CHAR = '+'
  private final val sequencePattern: Regex = "[A-Za-z]+".r
  private final val qualityPattern: Regex = "[!-~]+".r

  def setFastaReadAndReturnReadBytes(lineReader: LineReader, key: Text, value: FastaRead, fileName: String): Int = {
    value.clear()
    val buffer: Text = new Text()
    var readBytes = 0
    val namePrefix = if (value.isInstanceOf[FastqRead]) FASTQ_NAME_PREFIX_CHAR else FASTA_NAME_PREFIX_CHAR

    // key - sequence name
    readBytes += readLineInto(lineReader, buffer)
    if (buffer.toString.charAt(0) == namePrefix) key.set(new Text(buffer))
    else throw ReaderException(s"Unexpected character in read's name in file $fileName. Read key: ${key.toString}")

    value.setName(key)

    // sequence
    readBytes += readLineInto(lineReader, buffer)
    if (sequencePattern.pattern.matcher(buffer.toString).matches()) value.setSequence(new Text(buffer))
    else throw ReaderException(s"Unexpected character in read's sequence in in file $fileName. Read key: ${key.toString}")

    readBytes
  }

  def setFastqReadAndReturnReadBytes(lineReader: LineReader, key: Text, value: FastqRead, fileName: String): Int = {
    val buffer: Text = new Text()
    var readBytes = setFastaReadAndReturnReadBytes(lineReader, key, value, fileName)

    // separator
    readBytes += readLineInto(lineReader, buffer)
    if(buffer.getLength == 0 || buffer.charAt(0) != SEPARATOR_CHAR)
      throw ReaderException(s"Unexpected character in file $fileName. Read key: ${key.toString}. This should be '$SEPARATOR_CHAR' separator.")

    // quality
    readBytes += readLineInto(lineReader, buffer)
    if (qualityPattern.pattern.matcher(buffer.toString).matches()) value.setQuality(new Text(buffer))
    else throw ReaderException(s"Unexpected character in read's quality file $fileName. Read key: ${key.toString}")

    readBytes
  }

  private def readLineInto(lineReader: LineReader, destination: Text): Int = {
    val bytesRead = lineReader.readLine(destination, MAX_LINE_LENGTH)
    if (bytesRead <= 0) throw new EOFException
    bytesRead
  }
}
