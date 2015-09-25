package com.wilb0t.memcache

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.TimeUnit

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class ResponseSpec extends FunSpec with Matchers {

  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

  describe("toInt") {
    it("should return correct Int from byte array") {
      Response.toInt(Array(0x81,0x42,0x24,0x18), 0) should equal (0x81422418)
    }
  }

  describe("toLong") {
    it("should return correct Long from byte array") {
      val l = Response.toLong(Array(0x81,0x42,0x24,0x18,0x18,0x24,0x42,0x81), 0)
      l should equal (0x8142241818244281L)
    }
  }

  def byteStream(bytes: Array[Byte]): InputStream = new ByteArrayInputStream(bytes)

  describe("ResponseParser") {
    it("should return Success for a success server packet") {
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
      response should matchPattern {
        case Response.Success(Some("key"), 0x0807060504030201L, Some(Array(0x0))) => }
    }
  }
}
