package com.wilb0t.scam

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command extends InternalCommand { }

protected[scam]
trait InternalCommand {
  def opcode:   Byte
  def opaque:   Int
  def cas:      Option[Long]
  def extras:   Option[Array[Byte]]
  def keyBytes: Option[Array[Byte]]
  def value:    Option[Array[Byte]]
}

object Command {
  protected[scam]
  val magic: Byte = 0x80.toByte

  protected[scam]
  val headerLen: Int = 24

  protected[scam]
  val keyEncoding = Charset.forName("UTF-8")

  trait Setter extends Command {
    def flags: Int
    def exptime: Int
    def key: String
    def setvalue: Array[Byte]
    override val keyBytes = Some(key.getBytes(keyEncoding))
    override val opaque = 0x00
    override val extras = Some(Array(
      (flags >>> 24).toByte,
      (flags >>> 16).toByte,
      (flags >>> 8).toByte,
      flags.toByte,
      (exptime >>> 24).toByte,
      (exptime >>> 16).toByte,
      (exptime >>> 8).toByte,
      exptime.toByte))
    override val value = Some(setvalue)
  }

  case class Set(override val key: String,
                 override val flags: Int,
                 override val exptime: Int,
                 override val cas: Option[Long],
                 setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x01.toByte
  }

  protected[scam]
  case class SetQ(override val key: String,
                  override val flags: Int,
                  override val exptime: Int,
                  override val opaque: Int,
                  override val cas: Option[Long],
                  setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x11.toByte
  }

  case class Add(override val key: String,
                 override val flags: Int,
                 override val exptime: Int,
                 override val cas: Option[Long],
                 setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x02.toByte
  }

  protected[scam]
  case class AddQ(override val key: String,
                  override val flags: Int,
                  override val exptime: Int,
                  override val opaque: Int,
                  override val cas: Option[Long],
                  setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x12.toByte
  }

  case class Replace(override val key: String,
                     override val flags: Int,
                     override val exptime: Int,
                     override val cas: Option[Long],
                     setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x03.toByte
  }

  protected[scam]
  case class ReplaceQ(override val key: String,
                      override val flags: Int,
                      override val exptime: Int,
                      override val opaque: Int,
                      override val cas: Option[Long],
                      setvalue: Array[Byte])
    extends Setter {
    override val opcode = 0x13.toByte
  }

  case class Get(key: String) extends Command {
    override val opcode = 0x00.toByte
    override val opaque = 0x0
    override val cas    = None
    override val extras = None
    override val keyBytes = Some(key.getBytes(keyEncoding))
    override val value  = None
  }

  protected[scam]
  case class GetQ(key: String, override val opaque: Int) extends Command {
    override val opcode = 0x09.toByte
    override val cas    = None
    override val extras = None
    override val keyBytes = Some(key.getBytes(keyEncoding))
    override val value  = None
  }

  case class Delete(key: String) extends Command {
    override val opcode = 0x04.toByte
    override val opaque = 0x0
    override val cas    = None
    override val extras = None
    override val keyBytes = Some(key.getBytes(keyEncoding))
    override val value  = None
  }

  protected[scam]
  case class DeleteQ(key: String, override val opaque: Int) extends Command {
    override val opcode = 0x14.toByte
    override val cas    = None
    override val extras = None
    override val keyBytes = Some(key.getBytes(keyEncoding))
    override val value  = None
  }

  case class Noop(override val opaque: Int) extends Command {
    override val opcode = 0x0a.toByte
    override val cas    = None
    override val extras = None
    override val keyBytes = None
    override val value  = None
  }

  protected[scam]
  trait IncDec extends Command {
    override val opaque = 0x0
    override val cas    = None
    override val extras = Some(Array(
      (delta >>> 56).toByte,
      (delta >>> 48).toByte,
      (delta >>> 40).toByte,
      (delta >>> 32).toByte,
      (delta >>> 24).toByte,
      (delta >>> 16).toByte,
      (delta >>>  8).toByte,
      delta.toByte,
      (initialVal >>> 56).toByte,
      (initialVal >>> 48).toByte,
      (initialVal >>> 40).toByte,
      (initialVal >>> 32).toByte,
      (initialVal >>> 24).toByte,
      (initialVal >>> 16).toByte,
      (initialVal >>>  8).toByte,
      initialVal.toByte,
      (exptime >>> 24).toByte,
      (exptime >>> 16).toByte,
      (exptime >>> 8).toByte,
      exptime.toByte))
    override val keyBytes = Some(key.getBytes("UTF-8"))
    override val value = None

    def key: String
    def initialVal: Long
    def exptime: Int
    def delta: Long
  }

  case class Increment(key: String, delta: Long, initialVal: Long, exptime: Int)
    extends IncDec {
    override val opcode = 0x05.toByte
  }

  protected[scam]
  case class IncrementQ(key: String, delta: Long,
                        initialVal: Long,
                        exptime: Int,
                        override val opaque: Int)
    extends IncDec {
    override val opcode = 0x15.toByte
  }

  case class Decrement(key: String, delta: Long, initialVal: Long, exptime: Int)
    extends IncDec {
    override val opcode = 0x06.toByte
  }

  protected[scam]
  case class DecrementQ(key: String, delta: Long,
                        initialVal: Long,
                        exptime: Int,
                        override val opaque: Int)
    extends IncDec {
    override val opcode = 0x16.toByte
  }

  import scala.language.implicitConversions
  /**
   * Serializes Command to an Array[Bytes] suitable to be sent over a socket
   * to a memcached server
   *
   * @param cmd InternalCommand to serialize
   * @return Array[Byte]
   */
  implicit def toBytes(cmd: InternalCommand): Array[Byte] = {
    val extLen = cmd.extras.map{_.length}.getOrElse(0)
    val keyLen = cmd.keyBytes.map{_.length}.getOrElse(0)
    val valLen = cmd.value.map{_.length}.getOrElse(0)
    val bodyLen = extLen + keyLen + valLen
    val bytes = ByteBuffer.allocate(headerLen + bodyLen)
    val _cas = cmd.cas.getOrElse(0L)
    bytes
      .put(magic)
      .put(cmd.opcode)
      .put((keyLen >>> 8).toByte)
      .put(keyLen.toByte)
      .put(extLen.toByte)
      .put(0x0.toByte)
      .put(0x0.toByte)
      .put(0x0.toByte)
      .put((bodyLen >>> 24).toByte)
      .put((bodyLen >>> 16).toByte)
      .put((bodyLen >>> 8).toByte)
      .put(bodyLen.toByte)
      .put((cmd.opaque >>> 24).toByte)
      .put((cmd.opaque >>> 16).toByte)
      .put((cmd.opaque >>> 8).toByte)
      .put(cmd.opaque.toByte)
      .put((_cas >>> 56).toByte)
      .put((_cas >>> 48).toByte)
      .put((_cas >>> 40).toByte)
      .put((_cas >>> 32).toByte)
      .put((_cas >>> 24).toByte)
      .put((_cas >>> 16).toByte)
      .put((_cas >>> 8).toByte)
      .put(_cas.toByte)
    cmd.extras.foreach { bytes.put }
    cmd.keyBytes.foreach { bytes.put }
    cmd.value.foreach { bytes.put }

    bytes.array()
  }

  class QuietCommandException(msg: String = null, cause: Throwable = null)
    extends RuntimeException(msg, cause)

  class DefaultResponseException(msg: String = null, cause: Throwable = null)
    extends RuntimeException(msg, cause)

  protected[scam]
  def quietCommand(cmd: InternalCommand, opaque: Int): Command =
    cmd match {
      case Get(k)             => GetQ(k, opaque)
      case Set(k,f,e,c,v)     => SetQ(k, f, e, opaque, c, v)
      case Add(k,f,e,c,v)     => AddQ(k, f, e, opaque, c, v)
      case Replace(k,f,e,c,v) => ReplaceQ(k, f, e, opaque, c, v)
      case Delete(k)          => DeleteQ(k, opaque)
      case Increment(k,d,i,e) => IncrementQ(k,d,i,e,opaque)
      case Decrement(k,d,i,e) => DecrementQ(k,d,i,e,opaque)
      case _ => throw new QuietCommandException(s"No quiet command for $cmd")
    }

  protected[scam]
  def defaultResponse(cmd: InternalCommand): Response =
    cmd match {
      case GetQ(k,o)             => Response.KeyNotFound(Some(k))
      case SetQ(k,f,e,o,c,v)     => Response.Success(Some(k), 0, Some(v))
      case AddQ(k,f,e,o,c,v)     => Response.Success(Some(k), 0, Some(v))
      case ReplaceQ(k,f,e,o,c,v) => Response.Success(Some(k), 0, Some(v))
      case DeleteQ(k,o)          => Response.Success(Some(k), 0, None)
      case IncrementQ(k,d,i,e,o) => Response.Success(Some(k), 0, None)
      case DecrementQ(k,d,i,e,o) => Response.Success(Some(k), 0, None)
      case _ => throw new DefaultResponseException(s"No default response for $cmd")
    }
}

