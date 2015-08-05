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
    }
  }
}
