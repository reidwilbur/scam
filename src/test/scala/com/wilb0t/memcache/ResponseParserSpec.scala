package com.wilb0t.memcache

import java.io.{ByteArrayInputStream, InputStream}

import com.wilb0t.memcache.Response.ResponseParser
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseParserSpec extends FunSpec with Matchers with TryValues {
  describe("toInt") {
    it("should return correct Int from byte array") {
      Response.toInt(Array(0x81,0x42,0x24,0x18).map{_.toByte}, 0) should equal (0x81422418)
    }
  }

  describe("toLong") {
    it("should return correct Long from byte array") {
      val l = Response.toLong(Array(0x81,0x42,0x24,0x18,0x18,0x24,0x42,0x81).map{_.toByte}, 0)
      l should equal (0x8142241818244281L)
    }
  }

  def byteStream(bytes: Array[Byte]): InputStream = new ByteArrayInputStream(bytes)

  describe("ResponseParser") {
    it("should return Success for a success server packet") {
      val input = byteStream(Array(
       0x81, 0x01, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00,
       0x08, 0x07, 0x06, 0x05,
       0x04, 0x03, 0x02, 0x01
      ).map{_.toByte})

      new ResponseParser {}.parseResponse(input) should be (Response.Success(None, 0x0807060504030201L, None))
    }
  }
}
