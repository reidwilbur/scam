package com.wilb0t.memcache

import java.util.concurrent.TimeUnit

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import java.net.InetAddress

import scala.concurrent.duration.Duration
import scala.concurrent.{Awaitable, Await}

@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec with Matchers {

  // TODO(reid): these are actually integration tests since they require a live memcached server
  // should update maven to spin one up and put this test in a separate goal

  def blockForResult[T](f: Awaitable[T]): T = {
    Await.result(f, Duration(100, TimeUnit.MILLISECONDS))
  }

  describe("A Client") {
    it("should execute commands") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      implicit val timeout = Duration(10, TimeUnit.MILLISECONDS)

      val setResponse = blockForResult(
        client.execute(Set("somekey", 0, 3600, None, Array[Byte](0x0, 0x1, 0x2, 0x3)))
      )
      setResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x0,0x01,0x2,0x3))) => }

      val getResponse = blockForResult(
        client.execute(Get("somekey"))
      )
      getResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x0,0x1,0x2,0x3))) => }

      val addResponse = blockForResult(
        client.execute(Add("somekey", 0, 3600, None, Array[Byte](0x4,0x5)))
      )
      addResponse should matchPattern{ case Response.KeyExists(Some("somekey")) => }

      val replaceResponse = blockForResult(
        client.execute(Replace("somekey", 0, 3600, None, Array[Byte](0x7,0x8)))
      )
      replaceResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x7,0x8))) => }

      val getReplacedResponse = blockForResult(
        client.execute(Get("somekey"))
      )
      getReplacedResponse should matchPattern { case Response.Success(Some("somekey"), _, Some(Array(0x7,0x8))) => }

      val deleteResponse = blockForResult(
        client.execute(Delete("somekey"))
      )
      deleteResponse should matchPattern { case Response.Success(Some("somekey"), _, None) => }

      val getFailedResponse = blockForResult(
        client.execute(Get("somekey"))
      )
      getFailedResponse should matchPattern { case Response.KeyNotFound(Some("somekey")) => }

      client.execute(Delete("incKey"))

      val incInitResponse = blockForResult(
        client.execute(Increment("incKey", 0x35L, 0x0, 3600))
      )
      incInitResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0))) => }

      val incResponse = blockForResult(
        client.execute(Increment("incKey", 0x35L, 0x0, 3600))
      )
      incResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x35))) => }

      val decResponse = blockForResult(
        client.execute(Decrement("incKey", 0x01L, 0x0, 3600))
      )
      decResponse should matchPattern { case Response.Success(Some("incKey"), _, Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x34))) => }

      client.close()
    }

    it ("should return a value for each element of getM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      implicit val timeout = Duration(100, TimeUnit.MILLISECONDS)

      client.execute(Delete("somekey1"))
      client.execute(Delete("somekey2"))
      client.execute(Delete("somekey3"))
      client.execute(Delete("somekey4"))

      client.execute(Set("somekey1", 0, 3600, None, Array[Byte](0x1)))
      client.execute(Set("somekey2", 0, 3600, None, Array[Byte](0x2)))
      client.execute(Set("somekey3", 0, 3600, None, Array[Byte](0x3)))

      val getMResp = blockForResult(
        client.getM(List(
          Get("somekey4"),
          Get("somekey2"),
          Get("somekey3"),
          Get("somekey1")
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

    it ("should return a value for each element of setM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      implicit val timeout = Duration(100, TimeUnit.MILLISECONDS)

      val setMResp = blockForResult(
        client.setM(List(
          Set("somekey1", 0x0, 3600, None, Array[Byte](0x1)),
          Set("somekey2", 0x0, 3600, None, Array[Byte](0x2)),
          Set("somekey3", 0x0, 3600, None, Array[Byte](0x3)),
          Add("somekey3", 0x0, 3600, None, Array[Byte](0x4))
        ))
      )

      setMResp should matchPattern {
        case List(
        Response.Success(Some("somekey1"), _, _),
        Response.Success(Some("somekey2"), _, _),
        Response.Success(Some("somekey3"), _, _),
        Response.KeyExists(Some("somekey3"))
        ) =>
      }

      client.close()
    }

    it ("should return a value for each element of delM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      implicit val timeout = Duration(100, TimeUnit.MILLISECONDS)

      Set("somekey1", 0x0, 3600, None, Array[Byte](0x1))
      Set("somekey2", 0x0, 3600, None, Array[Byte](0x2))
      Set("somekey3", 0x0, 3600, None, Array[Byte](0x3))

      val delMResp = blockForResult(
        client.delM(List(
          Delete("somekey2"),
          Delete("somekey1"),
          Delete("doesntexist"),
          Delete("somekey3")
        ))
      )

      delMResp should matchPattern {
        case List(
          Response.Success(Some("somekey2"), _, _),
          Response.Success(Some("somekey1"), _, _),
          Response.KeyNotFound(Some("doesntexist")),
          Response.Success(Some("somekey3"), _, _)
        ) =>
      }

      client.close()
    }

    it("should return a value for each element of an incDecM") {
      val address = InetAddress.getByName("192.168.59.103")

      val client = Client(address, 11211).get

      implicit val timeout = Duration(100, TimeUnit.MILLISECONDS)

      client.delM(List(
        Delete("counter1"),
        Delete("counter2")
      ))

      val resp = blockForResult(
        client.incDecM(List(
          Increment("counter1", 0x1, 0x00, 3600),
          Increment("counter1", 0x1, 0x00, 3600),
          Decrement("counter1", 0x1, 0x00, 3600),
          Increment("counter2", 0x1, 0x10, 3600)
        ))
      )

      resp should matchPattern {
        case List(
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter1"), _, _),
          Response.Success(Some("counter2"), _, _)
        ) =>
      }

      val incResp = blockForResult(
        client.execute(Increment("counter1", 0x1, 0x00, 3600))
      )

      incResp should matchPattern { case Response.Success(_,_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x1))) => }

      val decResp = blockForResult(
        client.execute(Decrement("counter2", 0x1, 0x00, 3600))
      )

      decResp should matchPattern { case Response.Success(_,_,Some(Array(0x0,0x0,0x0,0x0,0x0,0x0,0x0,0xf))) => }

      client.close()
    }
  }
}
