package com.wilb0t.memcache

import java.io._
import java.util.concurrent.{TimeoutException, TimeUnit}

import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.concurrent.duration.Duration

class ResponseSpec extends FunSpec with MustMatchers with MockFactory {

  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

  def headerBytes(opcode: Byte, keyLen: Int, extraLen: Byte, status: Int, bodyLen: Int, opaque: Int, cas: Long)
    : Array[Byte] = {
    toByteArray(Array(
      0x81,       // magic number
      opcode,
      (keyLen >> 8).toByte, keyLen.toByte,
      extraLen,
      0x0,        // data type
      (status >> 8).toByte, status.toByte,
      (bodyLen >> 24).toByte, (bodyLen >> 16).toByte, (bodyLen >> 8).toByte, bodyLen.toByte,
      (opaque >> 24).toByte,  (opaque >> 16).toByte,  (opaque >> 8).toByte,  opaque.toByte,
      (cas >> 56).toByte, (cas >> 48).toByte, (cas >> 40).toByte, (cas >> 32).toByte,
      (cas >> 24).toByte, (cas >> 16).toByte, (cas >> 8).toByte,  cas.toByte
    ))
  }

  describe("Response") {
    describe("toInt") {
      it("must return correct Int for byte array") {
        Response.toInt(Array(0x81, 0x42, 0x24, 0x18), 0) must equal(0x81422418)
      }
    }

    describe("toLong") {
      it("must return correct Long for byte array") {
        val l = Response.toLong(Array(0x81, 0x42, 0x24, 0x18, 0x18, 0x24, 0x42, 0x81), 0)
        l must equal(0x8142241818244281L)
      }
    }
  }

  def byteStream(bytes: Array[Byte]): InputStream = new ByteArrayInputStream(bytes)

  describe("Response.Reader") {
    describe("readBytes") {
      implicit val timeout = Duration(5, TimeUnit.MILLISECONDS)

      it("must return correct bytes from InputStream") {
        val input = byteStream(Array(0x1, 0x2, 0x3, 0x4, 0x5))

        Response.Reader.readBytes(input, 5, timeout) must be(toByteArray(Array(0x1, 0x2, 0x3, 0x4, 0x5)))

        input.close()
      }

      it("must read only specified number of bytes from stream") {
        val input = byteStream(Array(
          0x81, 0x01, 0x02, 0x03,
          0xff
        ))

        Response.Reader.readBytes(input, 4, timeout) must be(toByteArray(Array(0x81, 0x01, 0x02, 0x03)))

        input.available() must be(1)
        input.read() must be(0xff)

        input.close()
      }

      it("must throw TimeoutException if timeout expires before all bytes are read") {
        val input = byteStream(Array(0x81, 0x01, 0x00, 0x00))

        a[TimeoutException] must be thrownBy
          Response.Reader.readBytes(input, Response.headerLen, Duration(1, TimeUnit.MILLISECONDS))

        input.close()
      }
    }

    describe("readPacket") {
      it("must return a packet for a server response with no body") {
        val input = mock[InputStream]
        val byteReader = mockFunction[InputStream, Int, Duration, Array[Byte]]

        val opcode = 1.toByte
        val keyLen = 0
        val extraLen = 0.toByte
        val status = 0.toByte
        val bodyLen = 0
        val opaque = 0x04030201
        val cas = 0x0807060504030201L
        byteReader.expects(input, 24, *).returns(headerBytes(opcode, keyLen, extraLen, status, bodyLen, opaque, cas))
        byteReader.expects(input, 0, *).returns(Array())

        val packet = Response.Reader.readPacket(byteReader)(input, Duration(1, TimeUnit.MILLISECONDS))

        packet.header.bodyLen must be(bodyLen)
        packet.header.magic must be(0x81.toByte)
        packet.header.status must be(status)
        packet.header.cas must be(cas)
        packet.header.extLen must be(extraLen)
        packet.header.opcode must be(opcode)
        packet.header.opaque must be(opaque)

        packet.extras must be(None)
        packet.key must be(None)
        packet.value must be(None)
      }

      it("must return a packet for a server response with a body") {
        val input = mock[InputStream]
        val byteReader = mockFunction[InputStream, Int, Duration, Array[Byte]]

        val opcode = 1.toByte
        val keyLen = 0
        val extraLen = 0.toByte
        val status = 0.toByte
        val bodyLen = 4
        val opaque = 0x04030201
        val cas = 0x0807060504030201L
        byteReader.expects(input, 24, *).returns(headerBytes(opcode, keyLen, extraLen, status, bodyLen, opaque, cas))
        byteReader.expects(input, 4, *).returns(Array(0x04, 0x03, 0x02, 0x01))

        val packet = Response.Reader.readPacket(byteReader)(input, Duration(1, TimeUnit.MILLISECONDS))

        packet.header.bodyLen must be(bodyLen)
        packet.header.magic must be(0x81.toByte)
        packet.header.status must be(status)
        packet.header.cas must be(cas)
        packet.header.extLen must be(extraLen)
        packet.header.opcode must be(opcode)
        packet.header.opaque must be(opaque)

        packet.extras must be(None)
        packet.key must be(None)
        packet.value must matchPattern { case Some(Array(0x04, 0x03, 0x02, 0x01)) => }
      }
    }

    describe("buildResponse") {
      it("must return Success for a success packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (cmd.keyBytes _).expects()
          .returns(Some("key".getBytes(Command.keyEncoding)))
          .atLeastOnce()
        (cmd.value _).expects().returns(None)

        (pkt.header _).expects().returns(hdr).atLeastOnce()
        (pkt.value _).expects().returns(None).atLeastOnce()

        (hdr.status _).expects().returns(0x0)
        (hdr.cas _).expects().returns(0x01)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must matchPattern{ case Response.Success(Some("key"), 0x1, None) => }
      }

      it("must return KeyNotFound for a key not found packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (cmd.keyBytes _).expects()
          .returns(Some("key".getBytes(Command.keyEncoding)))
          .atLeastOnce()

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x1)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must matchPattern{ case Response.KeyNotFound(Some("key")) => }
      }

      it("must return KeyExists for a key exists packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (cmd.keyBytes _).expects()
          .returns(Some("key".getBytes(Command.keyEncoding)))
          .atLeastOnce()

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x2)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must matchPattern{ case Response.KeyExists(Some("key")) => }
      }

      it("must return ValueTooLarge for a value too large packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x3)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.ValueTooLarge)
      }

      it("must return InvalidArguments for an invalid arguments packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x4)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.InvalidArguments)
      }

      it("must return ItemNotStored for an item not stored packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x5)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.ItemNotStored)
      }

      it("must return IncDevNonNumericValue for an inc dec non numeric value packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x6)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.IncDecNonNumericValue)
      }

      it("must return UnknownCommand for an unknown command packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x81)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.UnknownCommand)
      }

      it("must return OutOfMemory for an out of memory packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0x82)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.OutOfMemory)
      }

      it("must return UnknownServerResponse for an unknown packet") {
        val cmd = mock[InternalCommand]
        val pkt = mock[Response.Packet]
        val hdr = mock[Response.PacketHeader]

        (pkt.header _).expects().returns(hdr).atLeastOnce()

        (hdr.status _).expects().returns(0xff)

        val resp = Response.Reader.buildResponse(cmd, pkt)

        resp must be(Response.UnknownServerResponse)
      }
    }

    describe("PacketHeaderImpl") {
      it("must unpack byte array into correct fields") {
        val opcode = 1.toByte
        val keyLen = 0xff00
        val extraLen = 0xf0
        val status = 0xfeed
        val bodyLen = 0xfff0
        val opaque = 0x04030201
        val cas = 0x0807060504030201L

        val header = Response.PacketHeaderImpl(
          headerBytes(opcode, keyLen, extraLen.toByte, status, bodyLen, opaque, cas)
        )

        header.magic must be(0x81.toByte)
        header.opcode must be(opcode)
        header.keyLen must be(keyLen)
        header.extLen must be(extraLen)
        header.dataType must be(0x0)
        header.status must be(status)
        header.bodyLen must be(bodyLen)
        header.opaque must be(opaque)
        header.cas must be(cas)
      }
    }

    describe("PacketImpl") {
      it("must unpack extras from body based on PacketHeader") {
        val header = mock[Response.PacketHeader]

        (header.extLen _).expects().returns(4).atLeastOnce()
        (header.keyLen _).expects().returns(0).atLeastOnce()
        (header.bodyLen _).expects().returns(4).atLeastOnce()

        val packet = Response.PacketImpl(header, Array(0x01, 0x02, 0x03, 0x04))

        packet.extras must matchPattern { case Some(Array(0x01, 0x02, 0x03, 0x04)) => }
      }

      it("must unpack key from body based on PacketHeader") {
        val header = mock[Response.PacketHeader]

        (header.extLen _).expects().returns(4).atLeastOnce()
        (header.keyLen _).expects().returns(2).atLeastOnce()
        (header.bodyLen _).expects().returns(6).atLeastOnce()

        val packet = Response.PacketImpl(header, Array(0x01, 0x02, 0x03, 0x04, 0x05, 0x06))

        packet.key must matchPattern { case Some(Array(0x05, 0x06)) => }
      }

      it("must unpack value from body based on PacketHeader") {
        val header = mock[Response.PacketHeader]

        (header.extLen _).expects().returns(4).atLeastOnce()
        (header.keyLen _).expects().returns(2).atLeastOnce()
        (header.bodyLen _).expects().returns(8).atLeastOnce()

        val packet = Response.PacketImpl(header, Array(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08))

        packet.value must matchPattern { case Some(Array(0x07, 0x08)) => }
      }
    }
  }
}
