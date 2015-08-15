package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import java.net.InetAddress

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec with Matchers {
  describe("A Client") {
    it("should execute commands") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      val setResponse = Await.result(
        client.execute(Command.Set("somekey", 0, 3600, None, Array[Byte](0x0, 0x1, 0x2, 0x3))),
        Duration.Inf)

      setResponse should matchPattern { case Response.Success(None, _, None) => }

      val getResponse = Await.result(
        client.execute(Command.Get("somekey")),
        Duration.Inf)

      getResponse should matchPattern { case Response.Success(_, _, Some(Array(0x0,0x1,0x2,0x3))) => }

      val addResponse = Await.result(
        client.execute(Command.Add("somekey", 0, 3600, None, Array[Byte](0x4,0x5))),
        Duration.Inf)

      addResponse should matchPattern{ case Response.KeyExists() => }

      val replaceResponse = Await.result(
        client.execute(Command.Replace("somekey", 0, 3600, None, Array[Byte](0x7,0x8))),
        Duration.Inf
      )

      replaceResponse should matchPattern { case Response.Success(None, _, None) => }

      val getReplacedResponse = Await.result(
        client.execute(Command.Get("somekey")),
        Duration.Inf
      )

      getReplacedResponse should matchPattern { case Response.Success(_, _, Some(Array(0x7,0x8))) => }

      val deleteResponse = Await.result(
        client.execute(Command.Delete("somekey")),
        Duration.Inf
      )

      deleteResponse should matchPattern { case Response.Success(None, _, None) => }

      val getFailedResponse = Await.result(
        client.execute(Command.Get("somekey")),
        Duration.Inf)

      getFailedResponse should matchPattern { case Response.KeyNotFound() => }

      client.execute(Command.Delete("incKey"))

      val incInitResponse = Await.result(
        client.execute(Command.Increment("incKey", 0x35L, 0x0, 3600)),
        Duration.Inf)

      incInitResponse should matchPattern { case Response.Success(_, _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0))) => }

      val incResponse = Await.result(
        client.execute(Command.Increment("incKey", 0x35L, 0x0, 3600)),
        Duration.Inf)

      incResponse should matchPattern { case Response.Success(_, _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x35))) => }

      val decResponse = Await.result(
        client.execute(Command.Decrement("incKey", 0x01L, 0x0, 3600)),
        Duration.Inf)

      decResponse should matchPattern { case Response.Success(_, _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x34))) => }

      client.close()
    }

    it ("should return a value for each element of getM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      client.execute(Command.Delete("somekey1"))
      client.execute(Command.Delete("somekey2"))
      client.execute(Command.Delete("somekey3"))
      client.execute(Command.Delete("somekey4"))

      client.execute(Command.Set("somekey1", 0, 3600, None, Array[Byte](0x1)))
      client.execute(Command.Set("somekey2", 0, 3600, None, Array[Byte](0x2)))
      client.execute(Command.Set("somekey3", 0, 3600, None, Array[Byte](0x3)))

      val getMResp = Await.result(
        client.getM(List(
          Command.Get("somekey4"),
          Command.Get("somekey2"),
          Command.Get("somekey3"),
          Command.Get("somekey1")
        )),
        Duration.Inf
      )

      getMResp should matchPattern {
        case List(
          Response.KeyNotFound(),
          Response.Success(_, _, Some(Array(0x2))),
          Response.Success(_, _, Some(Array(0x3))),
          Response.Success(_, _, Some(Array(0x1)))
        ) =>
      }

      client.close()
    }

    it ("should return a value for each element of setM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      val setMResp = Await.result(
        client.setM(List(
          Command.Set("somekey1", 0x0, 3600, None, Array[Byte](0x1)),
          Command.Set("somekey2", 0x0, 3600, None, Array[Byte](0x2)),
          Command.Set("somekey3", 0x0, 3600, None, Array[Byte](0x3)),
          Command.Add("somekey3", 0x0, 3600, None, Array[Byte](0x4))
        )),
        Duration.Inf
      )

      setMResp should matchPattern {
        case List(
        Response.Success(Some("somekey1"), _, _),
        Response.Success(Some("somekey2"), _, _),
        Response.Success(Some("somekey3"), _, _),
        Response.KeyExists()
        ) =>
      }

      client.close()
    }

    it ("should return a value for each element of delM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      Command.Set("somekey1", 0x0, 3600, None, Array[Byte](0x1))
      Command.Set("somekey2", 0x0, 3600, None, Array[Byte](0x2))
      Command.Set("somekey3", 0x0, 3600, None, Array[Byte](0x3))

      val delMResp = Await.result(
        client.delM(List(
          Command.Delete("somekey2"),
          Command.Delete("somekey1"),
          Command.Delete("doesntexist"),
          Command.Delete("somekey3")
        )),
        Duration.Inf
      )

      delMResp should matchPattern {
        case List(
          Response.Success(Some("somekey2"), _, _),
          Response.Success(Some("somekey1"), _, _),
          Response.KeyNotFound(),
          Response.Success(Some("somekey3"), _, _)
        ) =>
      }

      client.close()
    }

    it("should return a value for each element of an incDecM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      client.delM(List(
        Command.Delete("counter1"),
        Command.Delete("counter2")
      ))

      val resp = Await.result(
        client.incDecM(List(
          Command.Increment("counter1", 0x1, 0x00, 3600),
          Command.Increment("counter1", 0x1, 0x00, 3600),
          Command.Decrement("counter1", 0x1, 0x00, 3600),
          Command.Increment("counter2", 0x1, 0x10, 3600)
        )),
        Duration.Inf)

      resp should matchPattern {
        case List(
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter2"), _, _)
        ) =>
      }

      val incResp = Await.result(
        client.execute(Command.Increment("counter1", 0x1, 0x00, 3600)),
        Duration.Inf
      )

      incResp should matchPattern { case Response.Success(_,_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x1))) => }

      val decResp = Await.result(
        client.execute(Command.Decrement("counter2", 0x1, 0x00, 3600)),
        Duration.Inf
      )

      decResp should matchPattern { case Response.Success(_,_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0xf))) => }

      client.close()
    }
  }
}
