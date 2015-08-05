package com.wilb0t.memcache

import java.io.InputStream

sealed trait Response

object Response {
  def toInt(bytes: Array[Byte], ofs: Int): Int =
    ((bytes(ofs) << 24) & 0xff000000) | ((bytes(ofs+1) << 16) & 0xff0000) | ((bytes(ofs+2) << 8) & 0xff00) | (bytes(ofs+3) & 0xff)

  def toLong(bytes: Array[Byte], ofs: Int): Long =
    ((toInt(bytes, ofs).toLong << 32) & 0xffffffff00000000L) | (toInt(bytes, ofs+4) & 0x0ffffffffL)

  val headerLen = 24

  protected case class PacketHeader(bytes: Array[Byte]) {

    val magic: Byte = bytes(0)
    val opcode: Byte = bytes(1)
    val keyLen: Int = ((bytes(2) << 8) & 0xff00) | (bytes(3) & 0x00ff)
    val extLen: Int = bytes(4) & 0xff
    def dataType: Byte = bytes(5)
    val status: Int = ((bytes(6) << 8) & 0xff00) | (bytes(7) & 0x00ff)
    val bodyLen: Int = toInt(bytes, 8)
    def opaque: Int  = toInt(bytes, 12)
    def cas: Long = toLong(bytes, 16)
  }

  protected case class Packet(header: PacketHeader, body: Array[Byte]) {
    def extras: Option[Array[Byte]] =
      if (header.extLen > 0) Some(body.slice(0, header.extLen)) else None

    def key: Option[Array[Byte]] =
      if (header.keyLen > 0) Some(body.slice(header.extLen, header.extLen + header.keyLen)) else None

    def value: Option[Array[Byte]] =
      if (header.extLen + header.keyLen < header.bodyLen) Some(body.slice(header.extLen + header.keyLen, header.bodyLen)) else None
  }

  trait ResponseParser {
    def parseResponse(input: InputStream): List[Response] = {
      val headerBytes = new Array[Byte](headerLen)
      val headerStatus = input.read(headerBytes, 0, headerLen)
      if (headerStatus != headerLen) throw new RuntimeException(s"Could not read packet header bytes, got $headerStatus expected $headerLen")
      val header = PacketHeader(headerBytes)

      val bodyBytes = new Array[Byte](header.bodyLen)
      if (header.bodyLen > 0) {
        val bodyStatus = input.read(bodyBytes, 0, header.bodyLen)
        if (bodyStatus != header.bodyLen) throw new RuntimeException(s"Could not read packet body bytes, got $bodyStatus expected ${header.bodyLen}")
      }
      val packet = Packet(header, bodyBytes)

      header.status match {
        case 0x00 => List(Success(header.opcode, header.cas))
        case 0x01 => List(KeyNotFound())
        case 0x02 => List(KeyExists())
        case 0x03 => List(ValueTooLarge())
        case 0x04 => List(InvalidArguments())
        case 0x05 => List(ItemNotStored())
        case 0x06 => List(IncDecNonNumericValue())
        case 0x81 => List(UnknownCommand())
        case 0x82 => List(OutOfMemory())
        case _ => List(UnknownServerResponse())
      }
    }
  }

  case class Success(opcode: Byte, cas: Long) extends Response {
  }

  case class KeyNotFound() extends Response {
  }

  case class KeyExists() extends Response {
  }

  case class ValueTooLarge() extends Response {

  }

  case class InvalidArguments() extends Response {

  }

  case class ItemNotStored() extends Response {

  }

  case class IncDecNonNumericValue() extends Response {

  }

  case class UnknownCommand() extends Response {

  }

  case class OutOfMemory() extends Response {

  }

  case class UnknownServerResponse() extends Response {

  }
}
