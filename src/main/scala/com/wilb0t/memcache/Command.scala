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

    val magic: Byte = 0x81.toByte

    val headerLen: Int = 24

    def serialize: Array[Byte] = {
      val extLen = extras.map { opts => opts.map {_.length}.sum }.getOrElse(0)
      val keyLen = key.map {_.length}.getOrElse(0)
      val valLen = value.map {_.length}.getOrElse(0)
      val bodyLen = extLen + keyLen + valLen
      val bytes = ByteBuffer.allocate(headerLen + bodyLen)
      bytes.put(0, magic)
      bytes.put(1, opcode)
      bytes.put(2, (keyLen >>> 8).toByte)
      bytes.put(3, keyLen.toByte)
      bytes.put(4, extLen.toByte)
      bytes.put(5, dataType)
      bytes.put(6, 0x0)
      bytes.put(7, 0x0)
      bytes.put(8,  (bodyLen >>> 24).toByte)
      bytes.put(9,  (bodyLen >>> 16).toByte)
      bytes.put(10, (bodyLen >>> 8).toByte)
      bytes.put(11, bodyLen.toByte)
      bytes.put(12, (opaque >>> 24).toByte)
      bytes.put(13, (opaque >>> 16).toByte)
      bytes.put(14, (opaque >>> 8).toByte)
      bytes.put(15, opaque.toByte)
      bytes.put(16, (cas >>> 56).toByte)
      bytes.put(17, (cas >>> 48).toByte)
      bytes.put(18, (cas >>> 40).toByte)
      bytes.put(19, (cas >>> 32).toByte)
      bytes.put(20, (cas >>> 24).toByte)
      bytes.put(21, (cas >>> 16).toByte)
      bytes.put(22, (cas >>> 8).toByte)
      bytes.put(23, cas.toByte)
      val keyOfs = extras.foldLeft(headerLen) {
        (o, arrays) =>
          arrays.foldLeft(o) {
            (ofs, array) =>
              bytes.put(array, ofs, array.length)
              ofs + array.length
          }
      }
      val valOfs = key.foldLeft(keyOfs) {
        (ofs, array) =>
          bytes.put(array, ofs, array.length)
          ofs + array.length
      }
      value.foreach { array => bytes.put(array, valOfs, array.length) }

      bytes.array()
    }
  }

  case class Set(key: String, flags: Int, exptime: Int, value: Array[Byte]) extends Command {
    def opcode: Byte = 0x01

    def serialize: Array[Byte] = {
      val expBytes = Array(
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

}
