package com.wilb0t.memcache

import java.io._
import java.util.concurrent.{TimeoutException, TimeUnit}

import com.wilb0t.memcache.Response.ByteReader
import org.scalatest._

import scala.concurrent.duration.Duration

class ResponseSpec extends FunSpec with MustMatchers {

  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

  val setNoBodySuccessResponse = toByteArray(Array(
    0x81,       // magic number
    0x01,       // opcode
    0x00, 0x00, // key length
    0x00,       // extra length
    0x00,       // data type
    0x00, 0x00, // status
    0x00, 0x00, 0x00, 0x00, // total body length
    0x00, 0x00, 0x00, 0x00, // opaque (sent from client)
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
        val input = byteStream(setNoBodySuccessResponse)

        Response.Reader.readBytes(input, Response.headerLen)(timeout) must be (setNoBodySuccessResponse)

        input.close()
      }

      it("must read only specified number of bytes from stream") {
        val input = byteStream(Array(
          0x81, 0x01, 0x02, 0x03,
          0xff
        ))

        Response.Reader.readBytes(input, 4)(timeout) must be (toByteArray(Array(0x81, 0x01, 0x02, 0x03)))

        input.available() must be (1)
        input.read() must be (0xff)

        input.close()
      }

      it("must throw TimeoutException if timeout expires before all bytes are read") {
        val input = byteStream(Array(0x81, 0x01, 0x00, 0x00))

        a [TimeoutException] must be thrownBy
          Response.Reader.readBytes(input, Response.headerLen)(Duration(1, TimeUnit.MILLISECONDS))

        input.close()
      }
      
      describe("readPacket") {
        it("should read packet for a server response with no body") {
          val inputStream = new ByteArrayInputStream(Array())

//          def readBytes(input: InputStream, numBytes: Int)(timeout: Duration): Array[Byte] = {
//            input must be theSameInstanceAs (inputStream)
//            //numBytes must be (24)
//            setNoBodySuccessResponse
//          }
//
//          val packet = Response.Reader.readPacket(readBytes)(inputStream)(Duration(1, TimeUnit.MILLISECONDS))
//
//          packet.header.bodyLen must be (0)
//          packet.header.magic must be (0x81)
//          packet.header.status must be (0x0)
//          packet.header.cas must be (0x8877665544332211L)
//          packet.header.extLen must be (0)
//          packet.header.opcode must be (0)
//          packet.extras must be (None)
//          packet.key must be (None)
//          packet.value must be (None)
          inputStream.close()
        }
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
