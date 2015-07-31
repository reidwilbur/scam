package com.wilb0t.memcache

import java.io.InputStream
import scala.annotation.tailrec
import scala.io.Source
import scala.collection.mutable.ListBuffer

sealed trait Response

object Response {
  import Command.Key

  case class Stored() extends Response
  case class NotStored() extends Response
  case class Exists() extends Response
  case class NotFound() extends Response
  case class Deleted() extends Response
  case class Touched() extends Response
  case class Value(key: Key, flags: Int, casUnique: Option[Long], value: List[Byte]) extends Response
  case class Error() extends Response
  case class ClientError(error: String) extends Response
  case class ServerError(error: String) extends Response

  trait Parser {
    def parseResponse(input: InputStream): List[Response]
  }

  trait StorageResponseParser extends Parser {
    override def parseResponse(input: InputStream): List[Response] = {
      Source.fromInputStream(input, "UTF-8").getLines().next() match {
        case "STORED" => List(Stored())
        case "NOT_STORED" => List(NotStored())
        case "EXISTS" => List(Exists())
        case "NOT_FOUND" => List(NotFound())
        case line => throw new IllegalArgumentException(s"Unable to parse response from server '$line'")
      }
    }
  }

  trait RetrievalResponseParser extends Parser {
    val endMatch = """^END$""".r
    val valMatch = """^VALUE (\w+) (\d+) (\d+)$""".r
    val valCasMatch = """^VALUE (\w+) (\d+) (\d+) (\d+)$""".r

    override def parseResponse(input: InputStream): List[Response] = {
      @tailrec
      def parse(i: Iterator[Char], buff: ListBuffer[Response]): List[Response] = {
        val (chars, rest) = i.span {
          _ != '\r'
        }
        chars.mkString("") match {
          case endMatch() =>
            rest.dropWhile{_ == '\r'}.dropWhile{_ == '\n'}
            buff.toList

          case valMatch(key, flags, bytes) =>
            val (data, toProcess) = rest.dropWhile {_ == '\r'}.dropWhile {_ == '\n'}.duplicate
            buff += Value(Key(key), flags.toInt, None, data.take(bytes.toInt).map {_.toByte}.toList)
            parse(toProcess.drop(bytes.toInt).dropWhile {_ == '\r'}.dropWhile {_ == '\n'}, buff)

          case valCasMatch(key, flags, bytes, casUnique) =>
            val (data, toProcess) = rest.dropWhile {_ == '\r'}.dropWhile {_ == '\n'}.duplicate
            buff += Value(Key(key), flags.toInt, Some(casUnique.toLong), data.take(bytes.toInt).map {_.toByte}.toList)
            parse(toProcess.drop(bytes.toInt).dropWhile {_ == '\r'}.dropWhile {_ == '\n'}, buff)

          case line => throw new IllegalArgumentException(s"Can't process server input '$line'")
        }
      }
      parse(Source.fromInputStream(input).buffered, ListBuffer[Response]())
    }
  }

}
