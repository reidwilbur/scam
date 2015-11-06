package com.wilb0t.scam

import org.scalatest.{Matchers, FunSpec}

class CommandSpec extends FunSpec with Matchers with ScamTest {

  describe("A Serialized Command") {
    it("must have the correct opcode byte for Get command") {
      Command.toBytes(Command.Get("key")).apply(1) should be (0x00)
    }

    it("must have the correct opcode byte for GetQ command") {
      Command.toBytes(Command.GetQ("key", 0x0)).apply(1) should be (0x09)
    }

    it("must have the correct opcode byte for Set command") {
      Command.toBytes(Command.Set("key", 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x01)
    }

    it("must have the correct opcode byte for SetQ command") {
      Command.toBytes(Command.SetQ("key", 0x0, 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x11)
    }

    it("must have the correct opcode byte for Add command") {
      Command.toBytes(Command.Add("key", 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x02)
    }

    it("must have the correct opcode byte for AddQ command") {
      Command.toBytes(Command.AddQ("key", 0x0, 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x12)
    }

    it("must have the correct opcode byte for Replace command") {
      Command.toBytes(Command.Replace("key", 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x03)
    }

    it("must have the correct opcode byte for ReplaceQ command") {
      Command.toBytes(Command.ReplaceQ("key", 0x0, 0x0, 0x0, None, Array(0x0))).apply(1) should be (0x13)
    }

    it("must have the correct opcode byte for Delete command") {
      Command.toBytes(Command.Delete("key")).apply(1) should be (0x04)
    }

    it("must have the correct opcode byte for DeleteQ command") {
      Command.toBytes(Command.DeleteQ("key", 0x0)).apply(1) should be (0x14)
    }

    it("must have the correct opcode byte for Increment command") {
      Command.toBytes(Command.Increment("key", 0x0L, 0x0L, 0x0)).apply(1) should be (0x05)
    }

    it("must have the correct opcode byte for IncrementQ command") {
      Command.toBytes(Command.IncrementQ("key", 0x0L, 0x0L, 0x0, 0x0)).apply(1) should be (0x15)
    }

    it("must have the correct opcode byte for Decrement command") {
      Command.toBytes(Command.Decrement("key", 0x0L, 0x0L, 0x0)).apply(1) should be (0x06)
    }

    it("must have the correct opcode byte for DecrementQ command") {
      Command.toBytes(Command.DecrementQ("key", 0x0L, 0x0L, 0x0, 0x0)).apply(1) should be (0x16)
    }

    it("must have the correct opcode byte for Noop command") {
      Command.toBytes(Command.Noop(0x0)).apply(1) should be (0x0a)
    }
  }

  describe("Command") {
    describe("toBytes") {
      it("must serialize a Setter Command correctly") {
        val cmd = Command.Set("key", 0xffeeddcc, 0xbbaa9988, Some(0xffeeddccbbaa9988L), Array(0xff, 0xee, 0xdd, 0xcc))

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Quiet Setter Command correctly") {
        val cmd = Command.SetQ("key", 0xffeeddcc, 0xbbaa9988, 0x11223344, Some(0xffeeddccbbaa9988L), Array(0xff, 0xee, 0xdd, 0xcc))

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Get Command correctly") {
        val cmd = Command.Get("key")

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Quiet Get Command correctly") {
        val cmd = Command.GetQ("key", 0xffffffff)

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Delete Command correctly") {
        val cmd = Command.Delete("key")

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Quiet Delete Command correctly") {
        val cmd = Command.DeleteQ("key", 0xffffffff)

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Noop Command correctly") {
        val cmd = Command.Noop(0xffffffff)

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize an IncDec Command correctly") {
        val cmd = Command.Increment("key", 0xffeeddccbbaa9988L, 0x7766554433221100L, 0x11223344)

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
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

      it("must serialize a Quiet IncDec Command correctly") {
        val cmd = Command.IncrementQ("key", 0xffeeddccbbaa9988L, 0x7766554433221100L, 0x11223344, 0x55667788)

        val bytes = Command.toBytes(cmd)

        bytes should be (toByteArray(Array(
          0x80,                   //magic
          0x15,                   //opcode
          0x00, 0x03,             //keylen
          0x14,                   //extras len
          0x00, 0x00, 0x00,       //reserved
          0x00, 0x00, 0x00, 0x17, //bodylen
          0x55, 0x66, 0x77, 0x88, //opaque value
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //cas
          0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, //extras delta
          0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, //extras initVal
          0x11, 0x22, 0x33, 0x44,  //extras exptime
          "key".charAt(0), "key".charAt(1), "key".charAt(2) //key
        )))
      }
    }

    describe("quietCommand") {
      it("must return GetQ for Get") {
        Command.quietCommand(Command.Get("key"), 0xab) should be
          Command.GetQ("key", 0xab)
      }

      it("must return SetQ for Set") {
        Command.quietCommand(Command.Set("key", 0x0, 0x1, Some(0x2), Array(0x1)), 0xab) should matchPattern {
          case Command.SetQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1)) =>
        }
      }

      it("must return AddQ for Add") {
        Command.quietCommand(Command.Add("key", 0x0, 0x1, Some(0x2), Array(0x1)), 0xab) should matchPattern {
          case Command.AddQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1)) =>
        }
      }

      it("must return ReplaceQ for Replace") {
        Command.quietCommand(Command.Replace("key", 0x0, 0x1, Some(0x2), Array(0x1)), 0xab) should matchPattern {
          case Command.ReplaceQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1)) =>
        }
      }

      it("must return DeleteQ for Delete") {
        Command.quietCommand(Command.Delete("key"), 0xab) should matchPattern {
          case Command.DeleteQ("key", 0xab) =>
        }
      }

      it("must return IncrementQ for Increment") {
        Command.quietCommand(Command.Increment("key", 0x0L, 0x1L, 0x2), 0xab) should matchPattern {
          case Command.IncrementQ("key", 0x0L, 0x1L, 0x02, 0xab) =>
        }
      }

      it("must return DecrementQ for Decrement") {
        Command.quietCommand(Command.Decrement("key", 0x0L, 0x1L, 0x2), 0xab) should matchPattern {
          case Command.DecrementQ("key", 0x0L, 0x1L, 0x02, 0xab) =>
        }
      }

      it("must throw QuietCommandException for unknown Command") {
        a [Command.QuietCommandException] should be thrownBy Command.quietCommand(Command.Noop(0xab), 0x12)
      }
    }

    describe("defaultResponse") {
      it("must be KeyNotFound for GetQ Command") {
        Command.defaultResponse(Command.GetQ("key", 0xab)) should matchPattern {
          case Response.KeyNotFound(Some("key")) =>
        }
      }

      it("must be Success for SetQ Command") {
        Command.defaultResponse(Command.SetQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1))) should matchPattern {
          case Response.Success(Some("key"), 0, Some(Array(0x1))) =>
        }
      }

      it("must be Success for AddQ Command") {
        Command.defaultResponse(Command.AddQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1))) should matchPattern {
          case Response.Success(Some("key"), 0, Some(Array(0x1))) =>
        }
      }

      it("must be Success for ReplaceQ Command") {
        Command.defaultResponse(Command.ReplaceQ("key", 0x0, 0x1, 0xab, Some(0x2), Array(0x1))) should matchPattern {
          case Response.Success(Some("key"), 0, Some(Array(0x1))) =>
        }
      }

      it("must be Success for DeleteQ Command") {
        Command.defaultResponse(Command.DeleteQ("key", 0xab)) should matchPattern {
          case Response.Success(Some("key"), 0, None) =>
        }
      }

      it("must be Success for IncrementQ Command") {
        Command.defaultResponse(Command.IncrementQ("key", 0x0L, 0x1L, 0x2, 0xab)) should matchPattern {
          case Response.Success(Some("key"), 0, None) =>
        }
      }

      it("must be Success for DecrementQ Command") {
        Command.defaultResponse(Command.DecrementQ("key", 0x0L, 0x1L, 0x2, 0xab)) should matchPattern {
          case Response.Success(Some("key"), 0, None) =>
        }
      }

      it("must throw DefaultResponseException for unknown Command") {
        a [Command.DefaultResponseException] should be thrownBy Command.defaultResponse(Command.Noop(0xab))
      }
    }
  }
}
