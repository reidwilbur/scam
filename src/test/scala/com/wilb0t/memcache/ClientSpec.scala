package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec with Matchers {
  import java.net.InetAddress

  import scala.concurrent.duration.Duration
  import scala.concurrent.{Await, ExecutionContext}

  import Response._
  import Command._

  describe("A Key") {
    it("should throw IllegalArgumentException when the value longer than 250") {
      intercept[IllegalArgumentException] {
        Key("a" * 251)
      }
    }

    it("should throw IllegalArgumentException when the value contains a space") {
      intercept[IllegalArgumentException] {
        Key(" ")
      }
    }

    it("should throw IllegalArgumentException when the value contains a tab") {
      intercept[IllegalArgumentException] {
        Key("\t")
      }
    }
  }

  describe("A Set command") {
    it("should serialize args to List[String]") {
      val s = Set(Key("somekey"), 0xffffffff, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))

      s.args should equal (List("set", "somekey", "65535", "3600", "4"))
    }

    it("should serialize to bytes") {
      val s = Set(Key("somekey"), 0xffffffff, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))

      s.toBytes.length should equal (26 + 4 + 2)
    }

  }

  describe("A Client") {
    it("should execute commands") {

      val address = InetAddress.getByName("192.168.59.103")

      import scala.concurrent.ExecutionContext.Implicits.global
      val client = Client(address, 11211).get

      val setResponse = Await.result(client.execute(Set(Key("somekey"), 0, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))), Duration.Inf)

      setResponse should have size (1)
      setResponse.head should equal (Stored())

      val getResponse = Await.result(client.execute(Get(List(Key("somekey")))), Duration.Inf)

      getResponse should have size (1)
      getResponse.head should equal (Value(Key("somekey"), 0, None, List[Byte](0x0, 0x1, 0x2, 0x3)))
    }
  }

}
