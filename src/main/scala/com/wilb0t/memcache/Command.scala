package com.wilb0t.memcache

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command {
  def opcode: Byte
  def opaque: Int
  def cas:    Option[Long]
  def extras: Option[Array[Byte]]
  def key:    Option[Array[Byte]]
  def value:  Option[Array[Byte]]

  val magic: Byte = 0x80.toByte

  val headerLen: Int = 24

  def serialize: Array[Byte] = {
    val extLen = extras.map{_.length}.getOrElse(0)
    val keyLen = key.map{_.length}.getOrElse(0)
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
    key.foreach { bytes.put }
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
      case _ => throw new RuntimeException(s"No quiet command for $cmd")
    }

  def defaultResponse(cmd: Command): Response =
    cmd match {
      case Command.GetQ(k,o) => Response.KeyNotFound()
      case Command.SetQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.AddQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.ReplaceQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.DeleteQ(k,o) => Response.Success(Some(k), 0x0, None)
      case cmd => throw new RuntimeException(s"No default response for $cmd")
    }

  trait Setter extends Command {
    def flags: Int
    def exptime: Int
    def setkey: String
    def setvalue: Array[Byte]
    override val key = Some(setkey.getBytes(keyEncoding))
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

  case class Set(override val setkey: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x01.toByte
  }

  protected case class SetQ(override val setkey: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x11.toByte
  }

  case class Add(override val setkey: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x02.toByte
  }

  protected case class AddQ(override val setkey: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x12.toByte
  }

  case class Replace(override val setkey: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x03.toByte
  }

  protected case class ReplaceQ(override val setkey: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x13.toByte
  }

  case class Get(getkey: String) extends Command {
    override val opcode = 0x00.toByte
    override val opaque = 0x0
    override val cas    = None
    override val extras = None
    override val key    = Some(getkey.getBytes(keyEncoding))
    override val value  = None
    def quietCommand(i: Int): Command = GetQ(getkey, i)
  }

  protected case class GetQ(getkey: String, override val opaque: Int) extends Command {
    override val opcode = 0x09.toByte
    override val cas    = None
    override val extras = None
    override val key    = Some(getkey.getBytes(keyEncoding))
    override val value  = None
  }

  case class Delete(delkey: String) extends Command {
    override val opcode = 0x04.toByte
    override val opaque = 0x0
    override val cas    = None
    override val extras = None
    override val key    = Some(delkey.getBytes(keyEncoding))
    override val value  = None
  }

  protected case class DeleteQ(delkey: String, override val opaque: Int) extends Command {
    override val opcode = 0x14.toByte
    override val cas    = None
    override val extras = None
    override val key    = Some(delkey.getBytes(keyEncoding))
    override val value  = None
  }

  case class Noop(override val opaque: Int) extends Command {
    override val opcode = 0x0a.toByte
    override val cas    = None
    override val extras = None
    override val key    = None
    override val value  = None
  }
}
