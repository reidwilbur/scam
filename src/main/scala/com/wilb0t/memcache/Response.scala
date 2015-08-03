package com.wilb0t.memcache

import java.io.InputStream
import scala.annotation.tailrec
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.util.Try

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
    def parseResponse(input: InputStream): Try[List[Response]]
  }

  object Error {
    val clientError = """^CLIENT_ERROR (.*)$""".r
    val serverError = """^SERVER_ERROR (.*)$""".r

    def unapply(s: String): Option[Response] =
      s match {
        case "ERROR" => Some(Error())
        case clientError(msg) => Some(ClientError(msg))
        case serverError(msg) => Some(ServerError(msg))
        case _ => None
      }
  }

  trait StorageResponseParser extends Parser {
    override def parseResponse(input: InputStream): Try[List[Response]] =
    Try (
      Source.fromInputStream(input, "UTF-8").getLines().next() match {
        case "STORED" => List(Stored())
        case "NOT_STORED" => List(NotStored())
        case "EXISTS" => List(Exists())
        case "NOT_FOUND" => List(NotFound())
        case Error(e) => List(e)
        case line => throw new IllegalArgumentException(s"Unable to parse response from server '$line'")
      }
    )
  }

  trait RetrievalResponseParser extends Parser {
    val end = """^END$""".r
    val value = """^VALUE (\w+) (\d+) (\d+)$""".r
    val valueWithCas = """^VALUE (\w+) (\d+) (\d+) (\d+)$""".r

    override def parseResponse(input: InputStream): Try[List[Response]] = {
      @tailrec
      def parse(i: Iterator[Char], buff: ListBuffer[Response]): List[Response] = {
        val (chars, rest) = i.span {_ != '\r'}
        chars.mkString("") match {
          case end() =>
            rest.dropWhile{_ == '\r'}.dropWhile{_ == '\n'}
            buff.toList

          case value(key, flags, bytes) =>
            val (data, toProcess) = rest.dropWhile {_ == '\r'}.dropWhile {_ == '\n'}.duplicate
            buff += Value(Key(key), flags.toInt, None, data.take(bytes.toInt).map {_.toByte}.toList)
            parse(toProcess.drop(bytes.toInt).dropWhile {_ == '\r'}.dropWhile {_ == '\n'}, buff)

          case valueWithCas(key, flags, bytes, casUnique) =>
            val (data, toProcess) = rest.dropWhile {_ == '\r'}.dropWhile {_ == '\n'}.duplicate
            buff += Value(Key(key), flags.toInt, Some(casUnique.toLong), data.take(bytes.toInt).map {_.toByte}.toList)
            parse(toProcess.drop(bytes.toInt).dropWhile {_ == '\r'}.dropWhile {_ == '\n'}, buff)

          case Error(e) => List(e)

          case line => throw new IllegalArgumentException(s"Can't process server input '$line'")
        }
      }

      Try(
        parse(Source.fromInputStream(input).buffered, ListBuffer[Response]())
      )
    }
  }

  trait DeleteResponseParser extends Parser {
    override def parseResponse(input: InputStream): Try[List[Response]] =
      Try (
        Source.fromInputStream(input, "UTF-8").getLines().next() match {
          case "DELETED" => List(Deleted())
          case "NOT_FOUND" => List(NotFound())
          case Error(e) => List(e)
          case line => throw new IllegalArgumentException(s"Unable to parse response from server '$line'")
        }
      )
  }

  trait TouchResponseParser extends Parser {
    override def parseResponse(input: InputStream): Try[List[Response]] =
      Try (
        Source.fromInputStream(input, "UTF-8").getLines().next() match {
          case "TOUCHED" => List(Touched())
          case "NOT_FOUND" => List(NotFound())
          case Error(e) => List(e)
          case line => throw new IllegalArgumentException(s"Unable to parse response from server '$line'")
        }
      )
  }
}
