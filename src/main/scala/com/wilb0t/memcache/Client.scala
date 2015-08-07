package com.wilb0t.memcache

import java.net.{Socket, InetAddress}
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try}

trait Client {
  def execute(command:Command) : Future[Try[List[Response]]]
}

object Client {
  def apply(address: InetAddress, port: Int): Try[Client] = {
    Try(
      new Client {
        val socket = new Socket(address, port)
        val in = socket.getInputStream
        val out = socket.getOutputStream

        implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

        override def execute(command: Command): Future[Try[List[Response]]] = Future {
          Try {
            out.write(command.serialize)
            out.flush()
            command.parseResponse(in)
          }
        }
      }
    )
  }
}
