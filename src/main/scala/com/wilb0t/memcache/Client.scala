package com.wilb0t.memcache

import java.net.{Socket, InetAddress}
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try}

trait Client {
  def execute(command:Command) : Future[Response]
  def getM(keys: List[String]) : Future[List[Response]]
  def setM(keysExp: List[(String,Int,Array[Byte])]) : Future[List[Response]]
  def close(): Unit
}

object Client {
  def apply(address: InetAddress, port: Int): Try[Client] = {
    Try(
      new Client {
        val socket = new Socket(address, port)
        val in = socket.getInputStream
        val out = socket.getOutputStream

        implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

        override def execute(command: Command): Future[Response] = Future {
          out.write(command.serialize)
          out.flush()
          Response.Parser(in)._2
        }

        override def getM(keys: List[String]): Future[List[Response]] = Future {
          val taggedKeys = keys.zipWithIndex
          val last = taggedKeys.last
          val quietGets = taggedKeys.init.map { case (key, i) => Command.GetQ(key, i) }
          quietGets.foreach { get => out.write(get.serialize) }
          out.write(Command.Get(last._1, last._2).serialize)
          out.flush()
          val responses = Response.Parser(in, last._2)
          taggedKeys.map { case (key, i) => responses.getOrElse(i, Response.KeyNotFound())}
        }

        override def setM(keysExp: List[(String,Int,Array[Byte])]): Future[List[Response]] = Future {
          val taggedKeys = keysExp.zipWithIndex
          val last = taggedKeys.last
          val quietGets =
            taggedKeys.init.map { case ((key, exptime, value), i) => Command.SetQ(key, 0x0, exptime, i, None, value) } ++
            List(last).map { case ((key, exptime, value), i) => Command.Set(key, 0x0, exptime, i, None, value)}
          quietGets.foreach { get => out.write(get.serialize) }
          out.flush()
          val responses = Response.Parser(in, last._2)
          taggedKeys.map { case ((key, exptime, value), i) => responses.getOrElse(i, Response.Success(Some(key), 0x0, Some(value)))}
        }

        override def close(): Unit = {
          in.close()
          out.close()
          socket.close()
        }
      }
    )
  }
}
