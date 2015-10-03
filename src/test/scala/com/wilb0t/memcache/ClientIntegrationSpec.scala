package com.wilb0t.memcache

import java.util.concurrent.TimeUnit

import org.scalatest._
import java.net.InetAddress

import scala.concurrent.duration.Duration
import scala.concurrent.{Awaitable, Await}

class ClientIntegrationSpec extends FunSpec with Matchers {

  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

  implicit val timeout = Duration(100, TimeUnit.MILLISECONDS)

  // TODO(reid): these are actually integration tests since they require a live memcached server
  // should update maven to spin one up and put this test in a separate goal

  def blockForResult[T](f: Awaitable[T]): T = {
    Await.result(f, Duration(1, TimeUnit.SECONDS))
  }

  describe("A Client") {
    it("should execute commands in order") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      val setResponse = blockForResult(
        client.execute(Command.Set("somekey", 0, 3600, None, Array[Byte](0x0, 0x1, 0x2, 0x3)))
      )
      setResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x0,0x01,0x2,0x3))) => }

      val getResponse = blockForResult(
        client.execute(Command.Get("somekey"))
      )
      getResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x0,0x1,0x2,0x3))) => }

      val addResponse = blockForResult(
        client.execute(Command.Add("somekey", 0, 3600, None, Array[Byte](0x4,0x5)))
      )
      addResponse should matchPattern{ case Response.KeyExists(Some("somekey")) => }

      val replaceResponse = blockForResult(
        client.execute(Command.Replace("somekey", 0, 3600, None, Array[Byte](0x7,0x8)))
      )
      replaceResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x7,0x8))) => }

      val getReplacedResponse = blockForResult(
        client.execute(Command.Get("somekey"))
      )
      getReplacedResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x7,0x8))) => }

      val deleteResponse = blockForResult(
        client.execute(Command.Delete("somekey"))
      )
      deleteResponse should matchPattern { case Response.Success(Some("somekey"), _, None) => }

      val getFailedResponse = blockForResult(
        client.execute(Command.Get("somekey"))
      )
      getFailedResponse should matchPattern { case Response.KeyNotFound(Some("somekey")) => }

      client.execute(Command.Delete("incKey"))

      val incInitResponse = blockForResult(
        client.execute(Command.Increment("incKey", 0x35L, 0x0, 3600))
      )
      incInitResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0))) => }

      val incResponse = blockForResult(
        client.execute(Command.Increment("incKey", 0x35L, 0x0, 3600))
      )
      incResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x35))) => }

      val decResponse = blockForResult(
        client.execute(Command.Decrement("incKey", 0x01L, 0x0, 3600))
      )
      decResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x34))) => }

      client.close()
    }

    it ("should return a Response for each Command of getM ") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      client.execute(Command.Delete("somekey1"))
      client.execute(Command.Delete("somekey2"))
      client.execute(Command.Delete("somekey3"))
      client.execute(Command.Delete("somekey4"))

      client.execute(Command.Set("somekey1", 0, 3600, None, Array[Byte](0x1)))
      client.execute(Command.Set("somekey2", 0, 3600, None, Array[Byte](0x2)))
      client.execute(Command.Set("somekey3", 0, 3600, None, Array[Byte](0x3)))

      val getMResp = blockForResult(
        client.getM(List(
          Command.Get("somekey4"),
          Command.Get("somekey2"),
          Command.Get("somekey3"),
          Command.Get("somekey1")
        ))
      )

      getMResp should matchPattern {
        case List(
          Response.KeyNotFound(Some("somekey4")),
          Response.Success(Some("somekey2"), _, Some(Array(0x2))),
          Response.Success(Some("somekey3"), _, Some(Array(0x3))),
          Response.Success(Some("somekey1"), _, Some(Array(0x1)))
        ) =>
      }

      client.close()
    }

    it ("should return a Response for each Command of setM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      val setMResp = blockForResult(
        client.setM(List(
          Command.Set("somekey1", 0x0, 3600, None, Array(0x1)),
          Command.Set("somekey2", 0x0, 3600, None, Array(0x2)),
          Command.Set("somekey3", 0x0, 3600, None, Array(0x3)),
          Command.Add("somekey3", 0x0, 3600, None, Array(0x4))
        ))
      )

      setMResp should matchPattern {
        case List(
          Response.Success(Some("somekey1"), _, Some(Array(0x01))),
          Response.Success(Some("somekey2"), _, Some(Array(0x02))),
          Response.Success(Some("somekey3"), _, Some(Array(0x03))),
          Response.KeyExists(Some("somekey3"))
        ) =>
      }

      client.close()
    }

    it ("should return a Response for each Command of delM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      Set("somekey1", 0x0, 3600, None, Array(0x1))
      Set("somekey2", 0x0, 3600, None, Array(0x2))
      Set("somekey3", 0x0, 3600, None, Array(0x3))

      val delMResp = blockForResult(
        client.delM(List(
          Command.Delete("somekey2"),
          Command.Delete("somekey1"),
          Command.Delete("doesntexist"),
          Command.Delete("somekey3")
        ))
      )

      delMResp should matchPattern {
        case List(
          Response.Success(Some("somekey2"), _, None),
          Response.Success(Some("somekey1"), _, None),
          Response.KeyNotFound(Some("doesntexist")),
          Response.Success(Some("somekey3"), _, None)
        ) =>
      }

      client.close()
    }

    it("should return a Response for each Command of an incDecM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      client.delM(List(
        Command.Delete("counter1"),
        Command.Delete("counter2")
      ))

      val resp = blockForResult(
        client.incDecM(List(
          Command.Increment("counter1", 0x1, 0x00, 3600),
          Command.Increment("counter1", 0x1, 0x00, 3600),
          Command.Decrement("counter1", 0x1, 0x00, 3600),
          Command.Increment("counter2", 0x1, 0x10, 3600)
        ))
      )

      resp should matchPattern {
        case List(
          Response.Success(Some("counter1"), _, None),
          Response.Success(Some("counter1"), _, None),
          Response.Success(Some("counter1"), _, None),
          Response.Success(Some("counter2"), _, None)
        ) =>
      }

      val incResp = blockForResult(
        client.execute(Command.Increment("counter1", 0x1, 0x00, 3600))
      )

      incResp should matchPattern {
        case Response.Success(Some("counter1"),_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x1))) =>
      }

      val decResp = blockForResult(
        client.execute(Command.Decrement("counter2", 0x1, 0x00, 3600))
      )

      decResp should matchPattern {
        case Response.Success(Some("counter2"),_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0xf))) =>
      }

      client.close()
    }
  }
}
