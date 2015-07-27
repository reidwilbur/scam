package com.wilb0t

import java.io.{BufferedReader, InputStreamReader, DataInputStream}
import java.net.{Socket, InetAddress}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Success, Try}

trait MemcacheClient {
  def query(command:MemcacheClient.Command)(implicit ec:ExecutionContext) : Future[MemcacheClient.Response]
}

object MemcacheClient {
  val delimiter = Array[Byte](13,10)
  val separator = ' '

  case class Key(value: String) {
    if (value.length > 250 || value.contains(' ') || value.contains("\\t")) {
      throw new IllegalArgumentException("Keys must contain no whitespace and be 250 chars or less")
    }
  }

  sealed trait Command {
    def cmdArgs: List[String]
    def data: Option[Array[Byte]]
    def toBytes: Array[Byte] = {
      val lines = List[Option[Array[Byte]]](Some(cmdArgs.mkString(" ").getBytes(Charset.forName("UTF-8"))), data)
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

    override def cmdArgs: List[String] = {
      List(
        this.getClass.getSimpleName.toLowerCase,
        key.value,
        (flags & 0xffff).toString(),
        exptime.toString,
        value.length.toString
      )
    }

    override def data: Option[Array[Byte]] = Some(value)
  }

  case class Set    (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Add    (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Replace(override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Append (override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand
  case class Prepend(override val key:Key, override val flags:Int, override val exptime:Int, override val value: Array[Byte]) extends StorageCommand

  case class Cas(override val key:Key, override val flags:Int, override val exptime:Int, casUnique:Long, override val value: Array[Byte]) extends StorageCommand

  trait RetrievalCommand extends Command {
    def keys: List[Key]

    override def data = None

    override def cmdArgs: List[String] = {
        this.getClass.getSimpleName.toLowerCase :: keys.map{_.value}
    }
  }

  case class Get(override val keys: List[Key]) extends RetrievalCommand
  case class Gets(override val keys: List[Key]) extends RetrievalCommand

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

  case class Error() extends Response
  case class ClientError(error: String) extends Response
  case class ServerError(error: String) extends Response

  object Response {
    def apply(line:String) : Response =
      line match {
        case "STORED" => Stored()
        case "NOT_STORED" => NotStored()
        case "EXISTS" => Exists()
        case "NOT_FOUND" => NotFound()
        case default => throw new IllegalArgumentException("Unable to parse line from server '" + line + "'")
      }
  }

  def apply(address: InetAddress, port: Int) : Try[MemcacheClient] = {
    Try(
      // TODO: this impl is totally not thread safe
      new MemcacheClient {
        val socket = new Socket(address, port)
        val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
        val out = socket.getOutputStream

        override def query(command: Command)(implicit ec: ExecutionContext): Future[Response] = Future {
          out.write(command.toBytes)
          out.flush()
          //Source.fromInputStream(socket.getInputStream).getLines().
          Response(in.readLine())
        }
      }
    )
  }
}
