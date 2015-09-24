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
    /**
     * Returns map of Int to Response corresponding to the input Commands.  Note that the server may not return
     * responses for all commands (quiet commands), so there is no guarantee that the size of the response map
     * will match the input command map.
     *
     * @param input InputStream connected to memcached server
     * @param finalResponseTag indicates the opaque value to expect for the final packet in response pipeline.
     *                         The final Command must have been a non-quiet command to ensure the server will send at
     *                         least 1 response.  Otherwise this method will timeout waiting for server responses that
     *                         may never come.
     * @param commands Map of opaque/tag to Command, commands must have already been sent to server
     * @param timeout maximum time to wait for all input bytes to arrive and be processed
     * @return Map[Int,Response]
     */
    def apply(input: InputStream, finalResponseTag: Int, commands: Map[Int, Command])(timeout: Duration): Map[Int, Response] = {
      @tailrec
      def _parse(responses: Map[Int, Response]): Map[Int,Response] = {
        val packet = read(input)(timeout)
        packet match {
          case p if p.header.opaque == finalResponseTag =>
            commands.get(p.header.opaque).map{
              cmd => responses + ((p.header.opaque, toResponse(cmd, p)))
            }.getOrElse(responses)
          case p =>
            val resps = commands.get(p.header.opaque).map{
              cmd => responses + ((p.header.opaque, toResponse(cmd, p)))
            }.getOrElse(responses)
            _parse(resps)
        }
      }
      _parse(Map())
    }

    /**
     * Reads the input stream and returns a Response for the Command.  Throws a TimeoutException if unable to read
     * enough bytes in the given timeout Duration.
     *
     * This will always timeout if used to read the response of a quiet command that has no server response.
     *
     * @param input input stream connected to memcached server
     * @param cmd last command that was sent to server
     * @param timeout maximum time to wait for all input bytes to arrive and be processed
     * @return Response
     */
    def apply(input: InputStream, cmd: Command)(timeout: Duration): Response =
      toResponse(cmd, read(input)(timeout))
  }

  def toResponse(cmd: Command, packet: Packet): Response = {
    val header = packet.header
    header.status match {
      case 0x00 => Success(cmd.keyBytes.map{ new String(_, cmd.keyEncoding) }, header.cas, packet.value.orElse(cmd.value))
      case 0x01 => KeyNotFound(cmd.keyBytes.map{ new String(_, cmd.keyEncoding) })
      case 0x02 => KeyExists(cmd.keyBytes.map{ new String(_, cmd.keyEncoding)} )
      case 0x03 => ValueTooLarge
      case 0x04 => InvalidArguments
      case 0x05 => ItemNotStored
      case 0x06 => IncDecNonNumericValue
      case 0x81 => UnknownCommand
      case 0x82 => OutOfMemory
      case _    => UnknownServerResponse
    }
  }

  def read(input: InputStream)(timeout: Duration): Packet = {
    val startTime = System.currentTimeMillis()
    val headerBytes = read(input, headerLen)(timeout)
    val header = PacketHeader(headerBytes)

    val elapsed = Duration(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
    val bodyTimeout = (timeout - elapsed).max(Duration(0, TimeUnit.MILLISECONDS))
    val bodyBytes = read(input, header.bodyLen)(bodyTimeout)
    Packet(header, bodyBytes)
  }

  def read(input: InputStream, numBytes: Int)(timeout: Duration): Array[Byte] = {
    val startTime = System.currentTimeMillis()
    val bytes = new Array[Byte](numBytes)

    @tailrec
    def _read(offset: Int): Array[Byte] = {
      if (offset >= numBytes) {
        bytes
      } else {
        val elapsed = Duration(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
        // TODO(reid): should try to flush input on read failure? may break/confuse later commands
        // probably need to drop the connection since will be out of sync with server
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

  case class Success(key: Option[String], cas: Long, value: Option[Array[Byte]]) extends Response

  case class KeyNotFound(key: Option[String]) extends Response

  case class KeyExists(key: Option[String]) extends Response

  case object ValueTooLarge extends Response

  case object InvalidArguments extends Response

  case object ItemNotStored extends Response

  case object IncDecNonNumericValue extends Response

  case object UnknownCommand extends Response

  case object OutOfMemory extends Response

  case object UnknownServerResponse extends Response
}
