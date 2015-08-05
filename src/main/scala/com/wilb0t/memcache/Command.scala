package com.wilb0t.memcache

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command extends Response.ResponseParser {
  def serialize: Array[Byte]
}

object Command {

  protected case class Packet(
    opcode: Byte,
    opaque: Int,
    cas: Long,
    extras: Option[List[Array[Byte]]],
    key:    Option[Array[Byte]],
    value:  Option[Array[Byte]]) {

    val magic: Byte = 0x80.toByte

    val headerLen: Int = 24

    def serialize: Array[Byte] = {
      val extLen = extras.map { opts => opts.map {_.length}.sum }.getOrElse(0)
      val keyLen = key.map {_.length}.getOrElse(0)
      val valLen = value.map {_.length}.getOrElse(0)
      val bodyLen = extLen + keyLen + valLen
      val bytes = ByteBuffer.allocate(headerLen + bodyLen)
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
        .put((cas >>> 56).toByte)
        .put((cas >>> 48).toByte)
        .put((cas >>> 40).toByte)
        .put((cas >>> 32).toByte)
        .put((cas >>> 24).toByte)
        .put((cas >>> 16).toByte)
        .put((cas >>> 8).toByte)
        .put(cas.toByte)
      extras.foreach { (arrays) => arrays.foreach { bytes.put } }
      key.foreach { bytes.put }
      value.foreach { bytes.put }

      bytes.array()
    }
  }

  trait Setter {
    def opcode: Byte
    def key: String
    def flags: Int
    def exptime: Int
    def cas: Option[Long]
    def value: Array[Byte]

    def serialize: Array[Byte] = {
      val extBytes = Array(
        (flags >>> 24).toByte,
        (flags >>> 16).toByte,
        (flags >>> 8).toByte,
        flags.toByte,
        (exptime >>> 24).toByte,
        (exptime >>> 16).toByte,
        (exptime >>> 8).toByte,
        exptime.toByte
      )
      Packet(opcode, 0x0, cas.getOrElse(0), Some(List(extBytes)), Some(key.getBytes(Charset.forName("UTF-8"))), Some(value)).serialize
    }
  }

  case class Set(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], override val value: Array[Byte]) extends Command with Setter {
    override val opcode: Byte = 0x01
  }

  case class Add(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], override val value: Array[Byte]) extends Command with Setter {
    override val opcode: Byte = 0x02
  }

  case class Replace(override val key: String, override val flags: Int, override val exptime: Int, override val cas: Option[Long], override val value: Array[Byte]) extends Command with Setter {
    override val opcode: Byte = 0x03
  }

  case class Get(key: String) extends Command {
    def opcode: Byte = 0x00

    def serialize: Array[Byte] =
      Packet(opcode, 0x0, 0x0, None, Some(key.getBytes(Charset.forName("UTF-8"))), None).serialize
  }

  case class Delete(key: String) extends Command {
    def opcode: Byte = 0x04

    def serialize: Array[Byte] =
      Packet(opcode, 0x0, 0x0, None, Some(key.getBytes(Charset.forName("UTF-8"))), None).serialize
  }
}
