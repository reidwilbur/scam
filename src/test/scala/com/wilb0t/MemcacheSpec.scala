package com.wilb0t

import java.net.InetAddress

import com.wilb0t.MemcacheClient.{Stored, Response, Key}
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
        Key("\\t")
      }
    }
  }

  describe("A Set command") {
    it("must serialize args to List[String]") {
      val s = MemcacheClient.Set(Key("somekey"), 0xffffffff, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))

      s.cmdArgs must equal (List("set", "somekey", "65535", "3600", "4"))
    }

    it("must serialize to bytes") {
      val s = MemcacheClient.Set(Key("somekey"), 0xffffffff, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))

      s.toBytes.length must equal (26 + 4 + 2)
    }

  }

  describe("Delimiter") {
    it("must have correct value") {
      MemcacheClient.delimiter must equal (Array[Byte](13,10))
    }
  }

  describe("Response") {
    it("must return Stored from a stored server message") {
      Response("STORED") must equal (MemcacheClient.Stored())
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
        case Success(r) => assert(r == Stored())
        case Failure(t) => fail(t)
      }
    }
  }
}
