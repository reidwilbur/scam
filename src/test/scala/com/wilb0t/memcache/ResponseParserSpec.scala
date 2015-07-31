package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.Charset

import Command.Key

@RunWith(classOf[JUnitRunner])
class ResponseParserSpec extends FunSpec with Matchers with TryValues {
  def stringStream(string:String): InputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")))

  describe("storageResponseParser") {
    it("should return Stored for a STORED server message") {
      new Response.StorageResponseParser {}.parseResponse(stringStream("STORED\r\n"))
        .success.value should equal (List(Response.Stored()))
    }

    it("should return NotStored for a NOT_STORED server message") {
      new Response.StorageResponseParser {}.parseResponse(stringStream("NOT_STORED\r\n"))
        .success.value should equal (List(Response.NotStored()))
    }

    it("should return Exists for an EXISTS server message") {
      new Response.StorageResponseParser {}.parseResponse(stringStream("EXISTS\r\n"))
        .success.value should equal (List(Response.Exists()))
    }

    it("should return NotFound for a NOT_FOUND server message") {
      new Response.StorageResponseParser {}.parseResponse(stringStream("NOT_FOUND\r\n"))
        .success.value should equal (List(Response.NotFound()))
    }

    it("should fail with IllegalArgumentException for any unhandled server message") {
      new Response.StorageResponseParser {}.parseResponse(stringStream("adsf\r\n"))
        .failure.exception.getClass should be (classOf[IllegalArgumentException])
    }
  }

  describe("RetrievalResponseParser") {
    it("should return empty list for empty server response") {
      new Response.RetrievalResponseParser {}.parseResponse(stringStream("END\r\n"))
        .success.value should equal (List())
    }

    it("should return correct Value list for server response with single value") {
      val responses  = new Response.RetrievalResponseParser {}.parseResponse(stringStream("VALUE somekey 65535 4\r\n1234\r\nEND\r\n"))
      responses.success.value should equal (List(
        Response.Value(Key("somekey"), 65535, None, List[Byte]('1','2','3','4'))
      ))
    }

    it("should return correct Value list for server response with multiple values") {
      val responses = new Response.RetrievalResponseParser {}.parseResponse(stringStream("VALUE somekey 65535 4\r\n1234\r\nVALUE someotherkey 0 3 123\r\n123\r\nEND\r\n"))
      responses.success.value should equal (List(
        Response.Value(Key("somekey"), 65535, None, List[Byte]('1', '2', '3', '4')),
        Response.Value(Key("someotherkey"), 0, Some(123), List[Byte]('1', '2', '3'))
      ))
    }

    it("should fail with IllegalArgumentException for any unhandled server message") {
      new Response.RetrievalResponseParser {}.parseResponse(stringStream("adsf\r\n"))
        .failure.exception.getClass should be (classOf[IllegalArgumentException])
    }
  }

  describe("DeleteResponseParser") {
    it("should return NotFound for NOT_FOUND server message") {
      new Response.DeleteResponseParser {}.parseResponse(stringStream("NOT_FOUND\r\n"))
        .success.value should equal (List(Response.NotFound()))
    }

    it("should return Deleted for DELETED server message") {
      new Response.DeleteResponseParser {}.parseResponse(stringStream("DELETED\r\n"))
        .success.value should equal (List(Response.Deleted()))
    }

    it("should fail with IllegalArgumentException for any unhandled server message") {
      new Response.DeleteResponseParser {}.parseResponse(stringStream("adsf\r\n"))
        .failure.exception.getClass should be (classOf[IllegalArgumentException])
    }
  }

  describe("TouchResponseParser") {
    it("should return NotFound for NOT_FOUND server message") {
      new Response.TouchResponseParser {}.parseResponse(stringStream("NOT_FOUND\r\n"))
        .success.value should equal (List(Response.NotFound()))
    }

    it("should return Touched for TOUCHED server message") {
     new Response.TouchResponseParser {}.parseResponse(stringStream("TOUCHED\r\n"))
        .success.value should equal (List(Response.Touched()))
    }

    it("should fail with IllegalArgumentException for any unhandled server message") {
      new Response.TouchResponseParser {}.parseResponse(stringStream("adsf\r\n"))
        .failure.exception.getClass should be (classOf[IllegalArgumentException])
    }
  }
}
