package com.wilb0t.memcache

import java.io.InputStream

import scala.annotation.tailrec

sealed trait Response

object Response {

  def default(cmd: Command): Response =
    cmd match {
      case Command.GetQ(k,o) => Response.KeyNotFound()
      case Command.SetQ(k,f,e,o,c,v) => Response.Success(Some(k), c.getOrElse(0), Some(v))
      case Command.DeleteQ(k,o) => Response.Success(Some(k), 0x0, None)
      case cmd => throw new RuntimeException(s"No default response for $cmd")
    }

  def toInt(bytes: Array[Byte], ofs: Int): Int =
    ((bytes(ofs) << 24) & 0xff000000) | ((bytes(ofs+1) << 16) & 0xff0000) | ((bytes(ofs+2) << 8) & 0xff00) | (bytes(ofs+3) & 0xff)

  def toLong(bytes: Array[Byte], ofs: Int): Long =
    ((toInt(bytes, ofs).toLong << 32) & 0xffffffff00000000L) | (toInt(bytes, ofs+4) & 0x0ffffffffL)

  val headerLen = 24

  protected case class PacketHeader(bytes: Array[Byte]) {

    val magic: Byte    = bytes(0)
    val opcode: Byte   = bytes(1)
    val keyLen: Int    = ((bytes(2) << 8) & 0xff00) | (bytes(3) & 0x00ff)
    val extLen: Int    = bytes(4) & 0xff
    def dataType: Byte = bytes(5)
    val status: Int    = ((bytes(6) << 8) & 0xff00) | (bytes(7) & 0x00ff)
    val bodyLen: Int   = toInt(bytes, 8)
    def opaque: Int    = toInt(bytes, 12)
    def cas: Long      = toLong(bytes, 16)
  }

  protected case class Packet(header: PacketHeader, body: Array[Byte]) {
    def extras: Option[Array[Byte]] =
      if (header.extLen > 0) Some(body.slice(0, header.extLen)) else None

    def key: Option[Array[Byte]] =
      if (header.keyLen > 0) Some(body.slice(header.extLen, header.extLen + header.keyLen)) else None

    def value: Option[Array[Byte]] =
      if (header.extLen + header.keyLen < header.bodyLen) Some(body.slice(header.extLen + header.keyLen, header.bodyLen)) else None
  }

  case object Parser {
    def apply(input: InputStream, finalResponseTag: Int): Map[Int, Response] = {
      @tailrec
      def parse(responses: Map[Int, Response]): Map[Int,Response] = {
        Parser(input) match {
          case p@(t, _) if t == finalResponseTag => responses + p
          case p => parse(responses + p)
        }
      }
      parse(Map())
    }

    def apply(input: InputStream): (Int, Response) = {
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
        case 0x00 => (header.opaque, Success(packet.key.map{ new String(_, "UTF-8") }, header.cas, packet.value))
        case 0x01 => (header.opaque, KeyNotFound())
        case 0x02 => (header.opaque, KeyExists())
        case 0x03 => (header.opaque, ValueTooLarge())
        case 0x04 => (header.opaque, InvalidArguments())
        case 0x05 => (header.opaque, ItemNotStored())
        case 0x06 => (header.opaque, IncDecNonNumericValue())
        case 0x81 => (header.opaque, UnknownCommand())
        case 0x82 => (header.opaque, OutOfMemory())
        case _    => (header.opaque, UnknownServerResponse())
      }
    }
  }

  case class Success(key: Option[String], cas: Long, value: Option[Array[Byte]]) extends Response {
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
