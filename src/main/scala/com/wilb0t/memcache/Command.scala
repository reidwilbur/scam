package com.wilb0t.memcache

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command {
  def opcode: Byte
  def opaque: Int
  def cas:      Option[Long]
  def extras:   Option[Array[Byte]]
  def keyBytes: Option[Array[Byte]]
  def value:    Option[Array[Byte]]

  val magic: Byte = 0x80.toByte

  val headerLen: Int = 24

  def serialize: Array[Byte] = {
    val extLen = extras.map{_.length}.getOrElse(0)
    val keyLen = keyBytes.map{_.length}.getOrElse(0)
    val valLen = value.map{_.length}.getOrElse(0)
    val bodyLen = extLen + keyLen + valLen
    val bytes = ByteBuffer.allocate(headerLen + bodyLen)
    val _cas = cas.getOrElse(0L)
    bytes
      .put(magic)
      .put(opcode)
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
      .put((opaque >>> 24).toByte)
      .put((opaque >>> 16).toByte)
      .put((opaque >>> 8).toByte)
      .put(opaque.toByte)
      .put((_cas >>> 56).toByte)
      .put((_cas >>> 48).toByte)
      .put((_cas >>> 40).toByte)
      .put((_cas >>> 32).toByte)
      .put((_cas >>> 24).toByte)
      .put((_cas >>> 16).toByte)
      .put((_cas >>> 8).toByte)
      .put(_cas.toByte)
    extras.foreach { bytes.put }
    keyBytes.foreach { bytes.put }
    value.foreach { bytes.put }

    bytes.array()
  }
}

object Command {

  val keyEncoding = Charset.forName("UTF-8")

  def quietCommand(cmd: Command, opaque: Int): Command =
    cmd match {
      case Get(k) => GetQ(k, opaque)
      case Set(k,f,e,c,v) => SetQ(k, f, e, opaque, c, v)
      case Add(k,f,e,c,v) => AddQ(k, f, e, opaque, c, v)
      case Replace(k,f,e,c,v) => ReplaceQ(k, f, e, opaque, c, v)
      case Delete(k) => DeleteQ(k, opaque)
      case Increment(k,d,i,e) => IncrementQ(k,d,i,e)
      case Decrement(k,d,i,e) => DecrementQ(k,d,i,e)
      case _ => throw new RuntimeException(s"No quiet command for $cmd")
    }

  def defaultResponse(cmd: Command): Response =
    cmd match {
      case Command.GetQ(k,o) => Response.KeyNotFound()
      case Command.SetQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.AddQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.ReplaceQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.DeleteQ(k,o) => Response.Success(Some(k), 0x0, None)
      case Command.IncrementQ(k,d,i,e) => Response.Success(Some(k), 0x0, None)
      case Command.DecrementQ(k,d,i,e) => Response.Success(Some(k), 0x0, None)
      case _ => throw new RuntimeException(s"No default response for $cmd")
    }

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

  case class Set(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x01.toByte
  }

  protected case class SetQ(override val key: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x11.toByte
  }

  case class Add(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x02.toByte
  }

  protected case class AddQ(override val key: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x12.toByte
  }

  case class Replace(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x03.toByte
  }

  protected case class ReplaceQ(override val key: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
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

  protected case class GetQ(key: String, override val opaque: Int) extends Command {
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

  protected case class DeleteQ(key: String, override val opaque: Int) extends Command {
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

  case class Increment(key: String, delta: Long, initialVal: Long, exptime: Int) extends IncDec {
    override val opcode = 0x05.toByte
  }

  case class IncrementQ(key: String, delta: Long, initialVal: Long, exptime: Int) extends IncDec {
    override val opcode = 0x15.toByte
  }

  case class Decrement(key: String, delta: Long, initialVal: Long, exptime: Int) extends IncDec {
    override val opcode = 0x06.toByte
  }

  case class DecrementQ(key: String, delta: Long, initialVal: Long, exptime: Int) extends IncDec {
    override val opcode = 0x16.toByte
  }
}
