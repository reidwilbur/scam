package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FunSpec}

@RunWith(classOf[JUnitRunner])
class CommandSpec extends FunSpec with Matchers {
  describe("A Key") {
    it("should handle valid key names") {
      Command.Key("somekey").value should be ("somekey")
    }

    it("should throw IllegalArgumentException when the value longer than 250") {
      intercept[IllegalArgumentException] {
        Command.Key("a" * 251)
      }
    }

    it("should throw IllegalArgumentException when the value contains a space") {
      intercept[IllegalArgumentException] {
        Command.Key(" ")
      }
    }

    it("should throw IllegalArgumentException when the value contains a tab") {
      intercept[IllegalArgumentException] {
        Command.Key("\t")
      }
    }
  }

  describe("A StorageCommand") {
    it("should serialize args to List[String]") {
      val cmd = Command.Set(Command.Key("somekey"), 0xffffffff, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))

      cmd.args should equal (List("set", "somekey", "65535", "3600", "4"))
    }

    it("should serialize to bytes") {
      val cmd = Command.Set(Command.Key("somekey"), 0xffffffff, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))

      cmd.toBytes.length should equal (26 + 4 + 2)
    }

    it("should have data") {
      val cmd = Command.Set(Command.Key("somekey"), 0xffffffff, 3600, List[Byte](0x0, 0x1, 0x2, 0x3))

      cmd.data should be (Some(List[Byte](0x0,0x1,0x2,0x3)))
    }

  }

  describe("A Cas command") {
    it("should serialize args to List[String]") {
      val cmd = Command.Cas(Command.Key("somekey"), 0xffffffff, 3600, 123, List[Byte](0x0, 0x1, 0x2, 0x3))

      cmd.args should equal (List("cas", "somekey", "65535", "3600", "4", "123"))
    }
  }

  describe("A RetrievalCommand") {
    it("should serialize args to List[String]") {
      val cmd = Command.Get(List(Command.Key("somekey"), Command.Key("someotherkey")))

      cmd.args should equal (List("get", "somekey", "someotherkey"))
    }

    it("should not have data") {
      Command.Get(List(Command.Key("somekey"))).data should be (None)
    }
  }

  describe("A Delete command") {
    it("should serialize args to List[String]") {
      val cmd = Command.Delete(Command.Key("somekey"))

      cmd.args should equal (List("delete", "somekey"))
    }

    it("should not have any data") {
      val cmd = Command.Delete(Command.Key("somekey"))

      cmd.data should equal (None)
    }
  }

  describe("A Touch command") {
    it("should serialize args to List[String]") {
      val cmd = Command.Touch(Command.Key("somekey"), 3600)

      cmd.args should equal (List("touch", "somekey", "3600"))
    }

    it("should not have any data") {
      val cmd = Command.Touch(Command.Key("somekey"), 3600)

      cmd.data should equal (None)
    }
  }

}
