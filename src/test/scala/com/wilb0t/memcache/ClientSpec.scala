package com.wilb0t.memcache

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import java.net.InetAddress

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec with Matchers with TryValues {
  describe("A Client") {
    it("should execute commands") {
      val address = InetAddress.getByName("192.168.59.103")

      import scala.concurrent.ExecutionContext.Implicits.global
      val client = Client(address, 11211).get

      val setResponse = Await.result(
        client.execute(Command.Set("somekey", 0, 3600, Array[Byte](0x0, 0x1, 0x2, 0x3))),
        Duration.Inf)

      setResponse.success.value should matchPattern { case List(Response.Success(None, _, None)) => }

      val getResponse = Await.result(
        client.execute(Command.Get("somekey")),
        Duration.Inf)

      getResponse.success.value should matchPattern { case List(Response.Success(_, _, Some(Array(0x0,0x1,0x2,0x3)))) => }

      val deleteResponse = Await.result(
        client.execute(Command.Delete("somekey")),
        Duration.Inf
      )

      deleteResponse.success.value should matchPattern { case List(Response.Success(None, _, None)) => }

      val getFailedResponse = Await.result(
        client.execute(Command.Get("somekey")),
        Duration.Inf)

      getFailedResponse.success.value should matchPattern { case List(Response.KeyNotFound()) => }
    }
  }
}
