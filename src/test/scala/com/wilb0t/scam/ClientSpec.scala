package com.wilb0t.scam

import java.io.{OutputStream, InputStream}
import java.util.concurrent.TimeUnit

import org.scalamock.scalatest.MockFactory
import org.scalatest.{MustMatchers, FunSpec}

import scala.concurrent.duration.Duration
import scala.util.Success

class ClientSpec extends FunSpec with MustMatchers with MockFactory with ScamTest {
  import Command.toBytes

  describe("ClientImpl") {
    it("must return responses for multiple commands using quiet commands followed by a Noop command") {
      val conn = mock[Connection]
      val reader = mock[Response.Reader]
      val inStream = mock[InputStream]
      val outStream = mock[OutputStream]
      val client = new ClientImpl(conn, reader)

      val cmds = List(
        Command.Get("somekey"),
        Command.Get("someotherkey"),
        Command.Get("yetanotherkey")
      )

      val expClientCmds = Map(
        (0, Command.GetQ("somekey", 0)),
        (1, Command.GetQ("someotherkey", 1)),
        (2, Command.GetQ("yetanotherkey", 2))
      )

      val someOtherKeyVal = Array[Byte](0x0)

      val svrRespCmds = Map(
        (1, Response.Success(Some("someotherkey"), 0x0, Some(someOtherKeyVal)))
      )

      val expResps = List(
        Response.KeyNotFound(Some("somekey")),
        Response.Success(Some("someotherkey"), 0x0, Some(someOtherKeyVal)),
        Response.KeyNotFound(Some("yetanotherkey"))
      )

      (conn.streams _).expects().returns(Success((inStream, outStream)))

      //should write quiet versions of commands to output stream
      expClientCmds.values.foreach{ (c) =>
        (outStream.write(_: Array[Byte]))
          .expects(where[Array[Byte]]{ bytes => bytes.sameElements(toBytes(c)) })
          .returns()
      }
      //should write a noop to trigger any responses for commands and mark the end
      //of the pipeline
      (outStream.write(_: Array[Byte]))
        .expects(where[Array[Byte]]{ bytes => bytes.sameElements(toBytes(Command.Noop(3))) })
      (outStream.flush _).expects().returns()

      (reader.read(_: InputStream, _: Int, _: Map[Int, InternalCommand])(_: Duration))
        .expects(inStream, 3, expClientCmds, Duration(100, TimeUnit.MILLISECONDS))
        .returns(svrRespCmds)

      val resps = blockForResult(client.getM(cmds)(Duration(100, TimeUnit.MILLISECONDS)))

      resps must equal(expResps)
    }

    it("must return response for a single command") {
      val conn = mock[Connection]
      val reader = mock[Response.Reader]
      val inStream = mock[InputStream]
      val outStream = mock[OutputStream]
      val client = new ClientImpl(conn, reader)

      (conn.streams _).expects().returns(Success((inStream, outStream)))

      val cmd = Command.Get("somekey")

      val svrResp = Response.KeyNotFound(Some("somekey"))

      (outStream.write(_: Array[Byte]))
        .expects(where[Array[Byte]]{ bytes => bytes.sameElements(toBytes(cmd)) })
        .returns()
      (outStream.flush _).expects().returns()

      (reader.read(_: InputStream, _: InternalCommand)(_: Duration))
        .expects(inStream, cmd, Duration(100, TimeUnit.MILLISECONDS))
        .returns(svrResp)

      val resp = blockForResult(client.execute(cmd)(Duration(100, TimeUnit.MILLISECONDS)))

      resp must equal(svrResp)
    }
  }
}
