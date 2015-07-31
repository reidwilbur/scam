package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.Charset

import Response._
import Command.Key

@RunWith(classOf[JUnitRunner])
class ResponseParserSpec extends FunSpec with Matchers {
  def stringStream(string:String): InputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")))

  describe("storageResponseParser") {
    it("should return Stored for a STORED server message") {
      new StorageResponseParser {}.parseResponse(stringStream("STORED\r\n")) should equal (List(Stored()))
    }

    it("should return NotStored for a NOT_STORED server message") {
      new StorageResponseParser {}.parseResponse(stringStream("NOT_STORED\r\n")) should equal (List(NotStored()))
    }

    it("should return Exists for an EXISTS server message") {
      new StorageResponseParser {}.parseResponse(stringStream("EXISTS\r\n")) should equal (List(Exists()))
    }

    it("should return NotFound for a NOT_FOUND server message") {
      new StorageResponseParser {}.parseResponse(stringStream("NOT_FOUND\r\n")) should equal (List(NotFound()))
    }

    it("should throw IllegalArgumentException for any unhandled server message") {
      intercept[IllegalArgumentException] {
        new StorageResponseParser {}.parseResponse(stringStream("adsf\r\n"))
      }
    }
  }

  describe("RetrievalResponseParser") {
    it("should return empty list for empty server response") {
      new RetrievalResponseParser {}.parseResponse(stringStream("END\r\n")) should equal (List())
    }

    it("should return correct Value list for server response with single value") {
      val responses  = new RetrievalResponseParser {}.parseResponse(stringStream("VALUE somekey 65535 4\r\n1234\r\nEND\r\n"))
      responses should equal (List(
        Value(Key("somekey"), 65535, None, List[Byte]('1','2','3','4'))
      ))
    }

    it("should return correct Value list for server response with multiple values") {
      val responses = new RetrievalResponseParser {}.parseResponse(stringStream("VALUE somekey 65535 4\r\n1234\r\nVALUE someotherkey 0 3 123\r\n123\r\nEND\r\n"))
      responses should equal (List(
        Value(Key("somekey"), 65535, None, List[Byte]('1', '2', '3', '4')),
        Value(Key("someotherkey"), 0, Some(123), List[Byte]('1', '2', '3'))
      ))
    }
  }
}
