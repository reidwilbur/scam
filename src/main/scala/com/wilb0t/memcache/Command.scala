package com.wilb0t.memcache

import java.nio.ByteBuffer
import java.nio.charset.Charset

sealed trait Command extends Response.ResponseParser {
  def serialize: Array[Byte]
}

object Command {

  protected case class Packet(
    opcode:Byte,
    dataType:Byte,
    opaque:Int,
    cas:Long,
    extras:Option[List[Array[Byte]]],
    key:Option[Array[Byte]],
    value:Option[Array[Byte]]) {

    val magic: Byte = 0x80.toByte

    val headerLen: Int = 24

    def serialize: Array[Byte] = {
      val extLen = extras.map { opts => opts.map {_.length}.sum }.getOrElse(0)
      val keyLen = key.map {_.length}.getOrElse(0)
      val valLen = value.map {_.length}.getOrElse(0)
      val bodyLen = extLen + keyLen + valLen
      val bytes = ByteBuffer.allocate(headerLen + bodyLen)
      bytes.put(magic)
      bytes.put(opcode)
      bytes.put((keyLen >>> 8).toByte)
      bytes.put(keyLen.toByte)
      bytes.put(extLen.toByte)
      bytes.put(dataType)
      bytes.put(0x0.toByte)
      bytes.put(0x0.toByte)
      bytes.put( (bodyLen >>> 24).toByte)
      bytes.put( (bodyLen >>> 16).toByte)
      bytes.put( (bodyLen >>> 8).toByte)
      bytes.put( bodyLen.toByte)
      bytes.put( (opaque >>> 24).toByte)
      bytes.put( (opaque >>> 16).toByte)
      bytes.put( (opaque >>> 8).toByte)
      bytes.put( opaque.toByte)
      bytes.put( (cas >>> 56).toByte)
      bytes.put( (cas >>> 48).toByte)
      bytes.put( (cas >>> 40).toByte)
      bytes.put( (cas >>> 32).toByte)
      bytes.put( (cas >>> 24).toByte)
      bytes.put( (cas >>> 16).toByte)
      bytes.put( (cas >>> 8).toByte)
      bytes.put( cas.toByte)
      extras.foreach { (arrays) => arrays.foreach { bytes.put(_) } }
      key.foreach { bytes.put(_) }
      value.foreach { bytes.put(_) }

      bytes.array()
    }
  }

  case class Set(key: String, flags: Int, exptime: Int, value: Array[Byte]) extends Command {
    def opcode: Byte = 0x01

    def serialize: Array[Byte] = {
      val expBytes = Array(
        (flags >>> 24).toByte,
        (flags >>> 16).toByte,
        (flags >>> 8).toByte,
        flags.toByte,
        (exptime >>> 24).toByte,
        (exptime >>> 16).toByte,
        (exptime >>> 8).toByte,
        exptime.toByte
      )
      Packet(opcode, 0x0, 0x0, 0x0, Some(List(expBytes)), Some(key.getBytes(Charset.forName("UTF-8"))), Some(value)).serialize
    }
  }

  case class Get(key: String) extends Command {
    def opcode: Byte = 0x00

    def serialize: Array[Byte] =
      Packet(opcode, 0x0, 0x0, 0x0, None, Some(key.getBytes(Charset.forName("UTF-8"))), None).serialize
  }

  case class Delete(key: String) extends Command {
    def opcode: Byte = 0x04

    def serialize: Array[Byte] =
      Packet(opcode, 0x0, 0x0, 0x0, None, Some(key.getBytes(Charset.forName("UTF-8"))), None).serialize
  }
}
