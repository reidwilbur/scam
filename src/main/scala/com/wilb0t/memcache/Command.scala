package com.wilb0t.memcache

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command extends Response.Parser {
  val charset = Charset.forName("UTF-8")
  val delimiter = "\r\n".getBytes(charset)

  def args: List[String]
  def data: Option[List[Byte]]

  def toBytes: Array[Byte] = {
    val lines = List[Option[List[Byte]]](Some(args.mkString(" ").getBytes(charset).toList), data)
    val numBytes = lines.flatMap{_.map{_.length}}.sum
    val bytebuf = lines.foldLeft(ByteBuffer.allocate(numBytes + (2 * delimiter.length))) {
      case (buf, Some(bytes)) => buf.put(bytes.toArray).put(delimiter)
      case (buf, _) => buf
    }
    bytebuf.array()
  }
}

object Command {

  case class Key(value: String) {
    if (value.length > 250 || value.contains(' ') || value.contains('\t')) {
      throw new IllegalArgumentException("Keys must contain no whitespace and be 250 chars or less")
    }
  }

  trait StorageCommand extends Command with Response.StorageResponseParser {
    def key: Key
    def flags: Int
    def exptime: Int
    val value: List[Byte]

    override val args =
      List(
        this.getClass.getSimpleName.toLowerCase,
        key.value,
        (flags & 0xffff).toString(),
        exptime.toString,
        value.length.toString
      )

    override val data = Some(value)
  }

  case class Set(override val key: Key, override val flags: Int, override val exptime: Int, override val value: List[Byte]) extends StorageCommand
  case class Add(override val key: Key, override val flags: Int, override val exptime: Int, override val value: List[Byte]) extends StorageCommand
  case class Replace(override val key: Key, override val flags: Int, override val exptime: Int, override val value: List[Byte]) extends StorageCommand
  case class Append(override val key: Key, override val flags: Int, override val exptime: Int, override val value: List[Byte]) extends StorageCommand
  case class Prepend(override val key: Key, override val flags: Int, override val exptime: Int, override val value: List[Byte]) extends StorageCommand

  case class Cas(override val key: Key, override val flags: Int, override val exptime: Int, casUnique: Long, override val value: List[Byte]) extends StorageCommand {
    override val args =
      List(
        this.getClass.getSimpleName.toLowerCase,
        key.value,
        (flags & 0xffff).toString(),
        exptime.toString,
        value.length.toString,
        casUnique.toString
      )
  }

  trait RetrievalCommand extends Command with Response.RetrievalResponseParser {
    def keys: List[Key]

    override val data = None

    override val args = this.getClass.getSimpleName.toLowerCase :: keys.map {_.value}
  }

  case class Get(override val keys: List[Key]) extends RetrievalCommand

  case class Gets(override val keys: List[Key]) extends RetrievalCommand

  case class Delete(key:Key) extends Command with Response.DeleteResponseParser {
    override val data = None

    override val args = List("delete", key.value)
  }

  //  case class Increment(key:Key, value: Int) extends Command
  //  case class Decrement(key:Key, value: Int) extends Command

  case class Touch(key:Key, exptime:Int) extends Command with Response.TouchResponseParser {
    override val data = None

    override val args = List("touch", key.value, exptime.toString)
  }

}
