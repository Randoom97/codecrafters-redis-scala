package codecrafters_redis.utils

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.text.ParseException
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

sealed trait RedisType
object RedisType {
  case class SimpleString(simpleString: String) extends RedisType
  case class SimpleError(simpleError: String) extends RedisType
  case class Integer(integer: Int) extends RedisType
  case class BulkString(bulkString: Option[String]) extends RedisType
  case class Array(array: scala.Array[RedisType]) extends RedisType
  case class Null() extends RedisType
  case class Boolean(boolean: scala.Boolean) extends RedisType
  case class Double(double: scala.Double) extends RedisType
  case class BulkError(bulkError: String) extends RedisType
  case class VerbatimString(verbatimString: String) extends RedisType
  case class Push(push: scala.Array[RedisType]) extends RedisType
}

object RespParser {
  private def readToNextCRLF(stream: InputStream): Try[(Array[Byte], Int)] = Try {
    val result = ArrayBuffer[Byte]()
    var crFound = false
    var bytesRead = 0
    breakable {
      while (true) {
        val byte = stream.readNBytes(1).head
        bytesRead += 1
        byte match {
          case '\r' => crFound = true
          case '\n' => if (crFound) break() else result.append(byte)
          case _ =>
            if (crFound) result.append(byte)
            result.append(byte)
            crFound = false
        }
      }
    }
    (result.toArray, bytesRead)
  }

  private def scanString(stream: InputStream): Try[(String, Int)] = Try {
    val (bytes, bytesRead) = readToNextCRLF(stream).get
    (new String(bytes, StandardCharsets.UTF_8), bytesRead)
  }

  private def scanInt(stream: InputStream): Try[(Int, Int)] = Try {
    val (string, bytesRead) = scanString(stream).get
    (string.toInt, bytesRead)
  }

  private def bulkString(stream: InputStream): Try[(Option[String], Int)] = Try {
    val (length, bytesRead) = scanInt(stream).get
    if (length < 0)
      (None, bytesRead)
    else
      (Some(new String(stream.readNBytes(length + 2).take(length), StandardCharsets.UTF_8)), bytesRead + length)
  }

  def decode(stream: InputStream): Try[(RedisType, Int)] = Try {
    val typeByte = stream.readNBytes(1).head
    typeByte match {
      case '+' =>
        val (string, bytesRead) = scanString(stream).get
        (RedisType.SimpleString(string), bytesRead + 1)
      case '-' =>
        val (string, bytesRead) = scanString(stream).get
        (RedisType.SimpleError(string), bytesRead + 1)
      case ':' =>
        val (integer, bytesRead) = scanInt(stream).get
        (RedisType.Integer(integer), bytesRead + 1)
      case '$' =>
        val (string, bytesRead) = bulkString(stream).get
        (RedisType.BulkString(string), bytesRead + 1)
      case '*' =>
        var (length, bytesRead) = scanInt(stream).get
        val array = (0 until length).map(_ => {
          val (value, bytesReadForValue) = decode(stream).get
          bytesRead += bytesReadForValue
          value
        }).toArray
        (RedisType.Array(array), bytesRead + 1)
      case '_' =>
        stream.readNBytes(2) // skip the crlf for the next decode
        (RedisType.Null(), 3)
      case '#' =>
        val (bytes, bytesRead) = readToNextCRLF(stream).get
        if (bytes.length > 1) throw new ParseException("encoded boolean data was longer than expected", 2)
        bytes.head match {
          case 't' => (RedisType.Boolean(true), bytesRead + 1)
          case 'f' => (RedisType.Boolean(false), bytesRead + 1)
          case invalidByte => throw new ParseException(f"invalid type for boolean '$invalidByte%c'", 1)
        }
      case ',' =>
        val (string, bytesRead) = scanString(stream).get
        (RedisType.Double(string.toDouble), bytesRead + 1)
      case '!' =>
        val (stringOption, bytesRead) = bulkString(stream).get
        (RedisType.BulkError(stringOption.get), bytesRead + 1)
      case '=' =>
        val (stringOption, bytesRead) = bulkString(stream).get
        (RedisType.VerbatimString(stringOption.get), bytesRead + 1)
      case '>' =>
        var (length, bytesRead) = scanInt(stream).get
        val array = (0 until length).map(_ => {
          val (value, bytesReadForValue) = decode(stream).get
          bytesRead += bytesReadForValue
          value
        }).toArray
        (RedisType.Push(array), bytesRead + 1)
      case _ => throw new ParseException(f"data type '$typeByte%c' is not supported", 0)
    }
  }

  def encode(data: RedisType): String = {
    data match {
      case RedisType.SimpleString(simpleString) => encodeSimpleString(simpleString)
      case RedisType.SimpleError(simpleError) => encodeSimpleError(simpleError)
      case RedisType.Integer(integer) => encodeInteger(integer)
      case RedisType.BulkString(bulkString) => encodeBulkString(bulkString)
      case RedisType.Array(array) => encodeArray(array)
      case RedisType.Null() => encodeNull()
      case RedisType.Boolean(boolean) => encodeBoolean(boolean)
      case RedisType.Double(double) => encodeDouble(double)
      case RedisType.BulkError(bulkError) => encodeBulkError(bulkError)
      case RedisType.VerbatimString(verbatimString) => encodeVerbatimString(verbatimString)
      case RedisType.Push(push) => encodePush(push)
    }
  }

  def encodeSimpleString(simpleString: String): String = s"+$simpleString\r\n"

  def encodeSimpleError(simpleError: String): String = s"-$simpleError\r\n"

  def encodeInteger(integer: Int): String = s":$integer\r\n"

  def encodeBulkString(stringOption: Option[String]): String = {
    if (stringOption.isEmpty) {
      "$-1\r\n"
    } else {
      val string = stringOption.get
      s"$$${string.length}\r\n$string\r\n"
    }
  }

  def encodeArray(array: Array[RedisType]): String = {
    var result = s"*${array.length}\r\n"
    for (item <- array) result += encode(item)
    result
  }

  def encodeNull(): String = "_\r\n"

  def encodeBoolean(boolean: Boolean): String = s"#${if (boolean) 't' else 'f'}\r\n"

  def encodeDouble(double: Double): String = s",$double\r\n"

  def encodeBulkError(bulkError: String): String = s"=${bulkError.length}\r\n$bulkError\r\n"

  def encodeVerbatimString(verbatimString: String): String = s"=${verbatimString.length}\r\n$verbatimString\r\n"

  def encodePush(push: Array[RedisType]): String = {
    var result = s">${push.length}\r\n"
    for (item <- push) result += encode(item)
    result
  }
}
