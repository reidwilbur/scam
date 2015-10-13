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

  val setNoBodySuccessResponse = toByteArray(Array(
    0x81,       // magic number
    0x01,       // opcode
    0x00, 0x00, // key length
    0x00,       // extra length
    0x00,       // data type
    0x00, 0x00, // status
    0x00, 0x00, 0x00, 0x00, // total body length
    0x04, 0x03, 0x02, 0x01, // opaque (sent from client)
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01  // cas
  ))

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

        Response.Reader.readBytes(input, 5, timeout) must be (toByteArray(Array(0x1, 0x2, 0x3, 0x4, 0x5)))

        input.close()
      }

      it("must read only specified number of bytes from stream") {
        val input = byteStream(Array(
          0x81, 0x01, 0x02, 0x03,
          0xff
        ))

        Response.Reader.readBytes(input, 4, timeout) must be (toByteArray(Array(0x81, 0x01, 0x02, 0x03)))

        input.available() must be (1)
        input.read() must be (0xff)

        input.close()
      }

      it("must throw TimeoutException if timeout expires before all bytes are read") {
        val input = byteStream(Array(0x81, 0x01, 0x00, 0x00))

        a [TimeoutException] must be thrownBy
          Response.Reader.readBytes(input, Response.headerLen, Duration(1, TimeUnit.MILLISECONDS))

        input.close()
      }
      
      describe("readPacket") {
        it("should read packet for a server response with no body") {
          val input = mock[InputStream]
          val byteReader = mockFunction[InputStream,Int,Duration,Array[Byte]]

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

          packet.header.bodyLen must be (bodyLen)
          packet.header.magic   must be (0x81.toByte)
          packet.header.status  must be (status)
          packet.header.cas     must be (cas)
          packet.header.extLen  must be (extraLen)
          packet.header.opcode  must be (opcode)
          packet.header.opaque  must be (opaque)

          packet.extras must be (None)
          packet.key    must be (None)
          packet.value  must be (None)
        }
      }

      it("should read packet for a server response with a body") {
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
        packet.header.magic   must be(0x81.toByte)
        packet.header.status  must be(status)
        packet.header.cas     must be(cas)
        packet.header.extLen  must be(extraLen)
        packet.header.opcode  must be(opcode)
        packet.header.opaque  must be(opaque)

        packet.extras must be(None)
        packet.key    must be(None)
        packet.value  must matchPattern { case Some(Array(0x04, 0x03, 0x02, 0x01)) => }
      }
    }

    it("must return Success for a success server packet") {
      val cmd = Command.Set("key", 0x0, 0x0, None, Array(0x0))
      val input = byteStream(setNoBodySuccessResponse)

      val response = Response.Reader().read(input,cmd)(Duration(1, TimeUnit.MILLISECONDS))
      response must matchPattern {
        case Response.Success(Some("key"), 0x0807060504030201L, Some(Array(0x0))) => }

      input.close()
    }
  }
}
