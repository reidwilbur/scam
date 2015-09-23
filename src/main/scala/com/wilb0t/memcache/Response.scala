package com.wilb0t.memcache

import java.io.InputStream
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

sealed trait Response

object Response {

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
    def apply(input: InputStream, finalResponseTag: Int)(timeout: Duration): Map[Int, Response] = {
      @tailrec
      def parse(responses: Map[Int, Response]): Map[Int,Response] = {
        Parser(input)(timeout) match {
          case p@(t, _) if t == finalResponseTag => responses + p
          case p => parse(responses + p)
        }
      }
      parse(Map())
    }

    def apply(input: InputStream)(timeout: Duration): (Int, Response) = {
      val startTime = System.currentTimeMillis()
      val headerBytes = readWithTimeout(input, headerLen)(timeout)
      val header = PacketHeader(headerBytes)

      val elapsed = Duration(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
      val bodyTimeout = (timeout - elapsed).max(Duration(0, TimeUnit.MILLISECONDS))
      val bodyBytes = readWithTimeout(input, header.bodyLen)(bodyTimeout)
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

  def readWithTimeout(input: InputStream, numBytes: Int)(timeout: Duration): Array[Byte] = {
    val startTime = System.currentTimeMillis()
    val bytes = new Array[Byte](numBytes)

    @tailrec
    def _read(offset: Int): Array[Byte] = {
      if (offset >= numBytes) {
        bytes
      } else {
        val elapsed = Duration(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
        // TODO(reid): should try to flush input on read failure? may break confuse later commands
        if (elapsed >= timeout) throw new TimeoutException(s"Timed out reading $numBytes bytes after $timeout")

        val avail = input.available()
        if (avail > 0) {
          val bytesToRead = math.min(avail, bytes.length - offset)
          val read = input.read(bytes, offset, bytesToRead)

          if (read == -1)
            throw new RuntimeException(s"Got end of stream, expected $numBytes bytes, read $offset bytes")

          _read(offset + read)
        } else {
          _read(offset)
        }
      }
    }

    _read(0)
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
