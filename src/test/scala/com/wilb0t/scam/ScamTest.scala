package com.wilb0t.scam

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable}

trait ScamTest {
  import scala.language.implicitConversions
  implicit def toByteArray(bytes: Array[Int]): Array[Byte] = bytes.map{_.toByte}

  def blockForResult[T](f: Awaitable[T]): T = {
    Await.result(f, Duration(1, TimeUnit.SECONDS))
  }
}
