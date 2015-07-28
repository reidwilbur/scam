package com.wilb0t

import java.io.{InputStream, BufferedReader, InputStreamReader, DataInputStream}
import java.net.{Socket, InetAddress}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.matching.Regex
import scala.util.{Success, Try}

trait MemcacheClient {
  def query(command:MemcacheClient.Command)(implicit ec:ExecutionContext) : Future[List[MemcacheClient.Response]]
}

object MemcacheClient {
  val delimiter = "\r\n".getBytes(Charset.forName("UTF-8"))

  case class Key(value: String) {
    if (value.length > 250 || value.contains(' ') || value.contains("\t")) {
      throw new IllegalArgumentException("Keys must contain no whitespace and be 250 chars or less")
    }
  }

  trait ResponseParser {
    def apply(input: InputStream) : List[Response]
  }

  sealed trait Command {
    def args: List[String]
    def data: Option[Array[Byte]]
    def responseParser: ResponseParser

    def toBytes: Array[Byte] = {
      val lines = List[Option[Array[Byte]]](Some(args.mkString(" ").getBytes(Charset.forName("UTF-8"))), data)
      val length = lines.flatMap{_.map{_.length}}.sum
      val bytebuf = lines.foldLeft(ByteBuffer.allocate(length + (2 * delimiter.length))) {
        case (buf, Some(bytes)) => buf.put(bytes).put(delimiter)
        case (buf, _) => buf
      }
      bytebuf.array()
    }
  }

  trait StorageCommand extends Command {
    def key: Key
    def flags: Int
    def exptime: Int
    def value: Array[Byte]

    override def args: List[String] = {
      List(
        this.getClass.getSimpleName.toLowerCase,
        key.value,
        (flags & 0xffff).toString(),
        exptime.toString,
        value.length.toString
      )
    }

    override val data: Option[Array[Byte]] = Some(value)

    override val responseParser = StorageResponseParser()
  }

  case class Set    (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Add    (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Replace(override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Append (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Prepend(override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand

  case class Cas(override val key:Key, override val flags:Int, override val exptime:Int, casUnique:Long, override val value: Array[Byte]) extends StorageCommand

  trait RetrievalCommand extends Command {
    def keys: List[Key]

    override val data = None

    override def args: List[String] =
      this.getClass.getSimpleName.toLowerCase :: keys.map{_.value}
  }

//  case class Get(override val keys: List[Key]) extends RetrievalCommand
//  case class Gets(override val keys: List[Key]) extends RetrievalCommand

//  case class Delete(key:Key) extends Command
//
//  case class Increment(key:Key, value: Int) extends Command
//  case class Decrement(key:Key, value: Int) extends Command
//
//  case class Touch(key:Key, exptime:Int) extends Command

  sealed trait Response

  case class Stored() extends Response
  case class NotStored() extends Response
  case class Exists() extends Response
  case class NotFound() extends Response
  case class Deleted() extends Response
  case class Touched() extends Response

  case class Value(key:Key, flags:Int, casUnique:Option[Long], value:Array[Byte]) extends Response

  case class Error() extends Response
  case class ClientError(error: String) extends Response
  case class ServerError(error: String) extends Response

  case class StorageResponseParser() extends ResponseParser {
    override def apply(input:InputStream) : List[Response] = {
      Source.fromInputStream(input, "UTF-8").getLines().next() match {
        case "STORED" => List(Stored())
        case "NOT_STORED" => List(NotStored())
        case "EXISTS" => List(Exists())
        case "NOT_FOUND" => List(NotFound())
        case line => throw new IllegalArgumentException("Unable to parse response from server '" + line + "'")
      }
    }
  }

  case class RetrievalResponseParser() extends ResponseParser {
    val valCasMatch = """^VALUE (\w+) (\d+) (\d+) (\d+)$""".r
    val valMatch = """^VALUE (\w+) (\d+) (\d+)$""".r
    val endMatch = """^END$""".r

    override def apply(input:InputStream): List[Response] = {
      List()
    }
  }

  def apply(address: InetAddress, port: Int) : Try[MemcacheClient] = {
    Try(
      // TODO: this impl is totally not thread safe
      new MemcacheClient {
        val socket = new Socket(address, port)
        val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
        val out = socket.getOutputStream

        override def query(command: Command)(implicit ec: ExecutionContext): Future[List[Response]] = Future {
          out.write(command.toBytes)
          out.flush()
          command.responseParser(socket.getInputStream)
        }
      }
    )
  }
}
