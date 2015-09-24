package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FunSpec}

@RunWith(classOf[JUnitRunner])
class CommandSpec extends FunSpec with Matchers {

  implicit def toByteArray(bytes: List[Int]): Array[Byte] = bytes.map{_.toByte}.toArray

  describe("Opcode") {
    it("should be correct for Get command") {
      Command.toBytes(Get("key")).apply(1) should be (0x00)
    }

    it("should be correct for GetQ command") {
      Command.toBytes(GetQ("key", 0x0)).apply(1) should be (0x09)
    }

    it("should be correct for Set command") {
      Command.toBytes(Set("key", 0x0, 0x0, None, List(0x0))).apply(1) should be (0x01)
    }

    it("should be correct for SetQ command") {
      Command.toBytes(SetQ("key", 0x0, 0x0, 0x0, None, List(0x0))).apply(1) should be (0x11)
    }

    it("should be correct for Add command") {
      Command.toBytes(Add("key", 0x0, 0x0, None, List(0x0))).apply(1) should be (0x02)
    }

    it("should be correct for AddQ command") {
      Command.toBytes(AddQ("key", 0x0, 0x0, 0x0, None, List(0x0))).apply(1) should be (0x12)
    }

    it("should be correct for Replace command") {
      Command.toBytes(Replace("key", 0x0, 0x0, None, List(0x0))).apply(1) should be (0x03)
    }

    it("should be correct for ReplaceQ command") {
      Command.toBytes(ReplaceQ("key", 0x0, 0x0, 0x0, None, List(0x0))).apply(1) should be (0x13)
    }

    it("should be correct for Delete command") {
      Command.toBytes(Delete("key")).apply(1) should be (0x04)
    }

    it("should be correct for DeleteQ command") {
      Command.toBytes(DeleteQ("key", 0x0)).apply(1) should be (0x14)
    }

    it("should be correct for Increment command") {
      Command.toBytes(Increment("key", 0x0L, 0x0L, 0x0)).apply(1) should be (0x05)
    }

    it("should be correct for IncrementQ command") {
      Command.toBytes(IncrementQ("key", 0x0L, 0x0L, 0x0, 0x0)).apply(1) should be (0x15)
    }

    it("should be correct for Decrement command") {
      Command.toBytes(Decrement("key", 0x0L, 0x0L, 0x0)).apply(1) should be (0x06)
    }

    it("should be correct for DecrementQ command") {
      Command.toBytes(DecrementQ("key", 0x0L, 0x0L, 0x0, 0x0)).apply(1) should be (0x16)
    }

    it("should be correct for Noop command") {
      Command.toBytes(Noop(0x0)).apply(1) should be (0x0a)
    }
  }

  describe("A Setter Command") {
    it("should serialize correctly") {
      val cmd = Set("key", 0xffeeddcc, 0xbbaa9988, Some(0xffeeddccbbaa9988L), List(0xff, 0xee, 0xdd, 0xcc))

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x01,                   //opcode
        0x00, 0x03,             //keylen
        0x08,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x0f, //bodylen
        0x00, 0x00, 0x00, 0x00, //opaque value
        0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, //cas
        0xff, 0xee, 0xdd, 0xcc, //flags
        0xbb, 0xaa, 0x99, 0x88, //exp time
        "key".charAt(0), "key".charAt(1), "key".charAt(2), //key
        0xff, 0xee, 0xdd, 0xcc  //value
      )))
    }
  }

  describe("A Quiet Setter Command") {
    it("should serialize correctly") {
      val cmd = SetQ("key", 0xffeeddcc, 0xbbaa9988, 0x11223344, Some(0xffeeddccbbaa9988L), List(0xff, 0xee, 0xdd, 0xcc))

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x11,                   //opcode
        0x00, 0x03,             //keylen
        0x08,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x0f, //bodylen
        0x11, 0x22, 0x33, 0x44, //opaque value
        0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, //cas
        0xff, 0xee, 0xdd, 0xcc, //flags
        0xbb, 0xaa, 0x99, 0x88, //exp time
        "key".charAt(0), "key".charAt(1), "key".charAt(2), //key
        0xff, 0xee, 0xdd, 0xcc  //value
      )))
    }
  }

  describe("A Get Command") {
    it("should serialize correctly") {
      val cmd = Get("key")

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x00,                   //opcode
        0x00, 0x03,             //keylen
        0x00,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x03, //bodylen
        0x00, 0x00, 0x00, 0x00, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
        "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
      )))
    }
  }

  describe("A GetQ Command") {
    it("should serialize correctly") {
      val cmd = GetQ("key", 0xffffffff)

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x09,                   //opcode
        0x00, 0x03,             //keylen
        0x00,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x03, //bodylen
        0xff, 0xff, 0xff, 0xff, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
        "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
      )))
    }
  }

  describe("A Delete Command") {
    it("should serialize correctly") {
      val cmd = Delete("key")

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x04,                   //opcode
        0x00, 0x03,             //keylen
        0x00,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x03, //bodylen
        0x00, 0x00, 0x00, 0x00, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
        "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
      )))
    }
  }

  describe("A DeleteQ Command") {
    it("should serialize correctly") {
      val cmd = DeleteQ("key", 0xffffffff)

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x14,                   //opcode
        0x00, 0x03,             //keylen
        0x00,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x03, //bodylen
        0xff, 0xff, 0xff, 0xff, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
        "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
      )))
    }
  }

  describe("A Noop Command") {
    it("should serialize correctly") {
      val cmd = Noop(0xffffffff)

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x0a,                   //opcode
        0x00, 0x00,             //keylen
        0x00,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x00, //bodylen
        0xff, 0xff, 0xff, 0xff, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 //cas
      )))
    }
  }

  describe("An IncDec Command") {
    it("should serialize correctly") {
      val cmd = Increment("key", 0xffeeddccbbaa9988L, 0x7766554433221100L, 0x11223344)

      val bytes = Command.toBytes(cmd)

      bytes should be (toByteArray(List(
        0x80,                   //magic
        0x05,                   //opcode
        0x00, 0x03,             //keylen
        0x14,                   //extras len
        0x00, 0x00, 0x00,       //reserved
        0x00, 0x00, 0x00, 0x17, //bodylen
        0x00, 0x00, 0x00, 0x00, //opaque value
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
        0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, //extras delta
        0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, //extras initVal
        0x11, 0x22, 0x33, 0x44,  //extras exptime
        "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
      )))
    }
  }
}
