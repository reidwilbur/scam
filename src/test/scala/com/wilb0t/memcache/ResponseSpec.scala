package com.wilb0t.memcache

import java.io._
import java.util.concurrent.{TimeoutException, TimeUnit}

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class ResponseSpec extends FunSpec with MustMatchers {

  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

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

  describe("Response.Parser") {
    describe("read") {
      implicit val timeout = Duration(1, TimeUnit.MILLISECONDS)

      it("must return correct bytes from InputStream") {
        val input = byteStream(Array(
          0x81, 0x01, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00,
          0x08, 0x07, 0x06, 0x05,
          0x04, 0x03, 0x02, 0x01
        ))

        Response.Parser.readBytes(input, Response.headerLen)(timeout) must be
          toByteArray(Array(
            0x81, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x08, 0x07, 0x06, 0x05,
            0x04, 0x03, 0x02, 0x01
          ))

        input.close()
      }

      it("must read only specified number of bytes from stream") {
        val input = byteStream(Array(
          0x81, 0x01, 0x02, 0x03,
          0xff
        ))

        Response.Parser.readBytes(input, 4)(timeout) must be (toByteArray(Array(0x81, 0x01, 0x02, 0x03)))

        input.available() must be (1)
        input.read() must be (0xff)

        input.close()
      }

      it("must throw TimeoutException if timeout expires before all bytes are read") {
        val input = byteStream(Array(0x81, 0x01, 0x00, 0x00))

        a [TimeoutException] must be thrownBy
          Response.Parser.readBytes(input, Response.headerLen)(Duration(10, TimeUnit.MILLISECONDS))

        input.close()
      }
    }

    it("must return Success for a success server packet") {
      val cmd = Command.Set("key", 0x0, 0x0, None, Array(0x0))
      val input = byteStream(Array(
        0x81, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x08, 0x07, 0x06, 0x05,
        0x04, 0x03, 0x02, 0x01
      ))

      val response = Response.Parser(input,cmd)(Duration(1, TimeUnit.MILLISECONDS))
      response must matchPattern {
        case Response.Success(Some("key"), 0x0807060504030201L, Some(Array(0x0))) => }

      input.close()
    }
  }
}
