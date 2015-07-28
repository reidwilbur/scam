package com.wilb0t

import java.io.{ByteArrayInputStream, InputStream}
import java.net.InetAddress
import java.nio.charset.Charset

import com.wilb0t.MemcacheClient.{StorageResponseParser, Stored, Response, Key}
import org.scalatest.{MustMatchers, Matchers, FunSpec}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.util.{Try, Failure, Success}

class MemcacheSpec extends FunSpec with MustMatchers {
  describe("A Key") {
    it("must throw IllegalArgumentException when the value longer than 250") {
      intercept[IllegalArgumentException] {
        Key("a" * 251)
      }
    }

    it("must throw IllegalArgumentException when the value contains a space") {
      intercept[IllegalArgumentException] {
        Key(" ")
      }
    }

    it("must throw IllegalArgumentException when the value contains a tab") {
      intercept[IllegalArgumentException] {
        Key("\t")
      }
    }
  }

  describe("A Set command") {
    it("must serialize args to List[String]") {
      val s = MemcacheClient.Set(Key("somekey"), 0xffffffff, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))

      s.args must equal (List("set", "somekey", "65535", "3600", "4"))
    }

    it("must serialize to bytes") {
      val s = MemcacheClient.Set(Key("somekey"), 0xffffffff, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))

      s.toBytes.length must equal (26 + 4 + 2)
    }

  }

  describe("StorageResponseParser") {
    it("must return Stored for a STORED server message") {
      val iss = new ByteArrayInputStream("STORED\r\n".getBytes(Charset.forName("UTF-8")))
      StorageResponseParser()(iss) must equal (List(MemcacheClient.Stored()))
    }

    it("must return NotStored for a NOT_STORED server message") {
      val iss = new ByteArrayInputStream("NOT_STORED\r\n".getBytes(Charset.forName("UTF-8")))
      StorageResponseParser()(iss) must equal (List(MemcacheClient.NotStored()))
    }

    it("must return Exists for an EXISTS server message") {
      val iss = new ByteArrayInputStream("EXISTS\r\n".getBytes(Charset.forName("UTF-8")))
      StorageResponseParser()(iss) must equal (List(MemcacheClient.Exists()))
    }

    it("must return NotFound for a NOT_FOUND server message") {
      val iss = new ByteArrayInputStream("NOT_FOUND\r\n".getBytes(Charset.forName("UTF-8")))
      StorageResponseParser()(iss) must equal (List(MemcacheClient.NotFound()))
    }

    it("must throw IllegalArgumentException for any unhandled server message") {
      val iss = new ByteArrayInputStream("asdf\r\n".getBytes(Charset.forName("UTF-8")))
      intercept[IllegalArgumentException] {
        StorageResponseParser()(iss)
      }
    }
  }

  describe("A MemcacheClient") {
    it("must execute a command") {
      val address = InetAddress.getByName("192.168.59.103")

      import ExecutionContext.Implicits.global
      val client = MemcacheClient(address, 11211)
      val setResponse = for {
        c <- client
      } yield Await.result(c.query(MemcacheClient.Set(Key("somekey"), 0x0, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))), Duration.Inf)

      setResponse match {
        case Success(r :: _) => r must equal (Stored())
        case Success(r) => fail("Unexpected response" + r)
        case Failure(t) => fail(t)
      }
    }
  }
}
