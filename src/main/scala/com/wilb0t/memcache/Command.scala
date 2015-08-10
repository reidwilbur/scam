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

  trait QuietCommand

  case class Set(override val setkey: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x01.toByte
  }

  case class SetQ(override val setkey: String, override val flags: Int, override val exptime: Int, override val opaque: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x11.toByte
  }

  case class Add(override val setkey: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x02.toByte
  }

  case class Replace(override val setkey: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], setvalue: Array[Byte]) extends Setter {
    override val opcode = 0x03.toByte
  }

  case class Get(getkey: String, override val opaque: Int) extends Command {
    override val opcode = 0x00.toByte
    override val cas    = None
    override val extras = None
    override val key    = Some(getkey.getBytes(keyEncoding))
    override val value  = None
  }

  case class GetQ(getkey: String, override val opaque: Int) extends Command with QuietCommand {
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
}
