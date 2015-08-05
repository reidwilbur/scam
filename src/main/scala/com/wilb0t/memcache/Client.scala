package com.wilb0t.memcache

import java.net.{Socket, InetAddress}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try}

trait Client {
  def execute(command:Command)(implicit ec:ExecutionContext) : Future[Try[List[Response]]]
}

object Client {
  def apply(address: InetAddress, port: Int): Try[Client] = {
    Try(
      // TODO: this impl is totally not thread safe
      new Client {
        val socket = new Socket(address, port)
        val in = socket.getInputStream
        val out = socket.getOutputStream

        override def execute(command: Command)(implicit ec: ExecutionContext): Future[Try[List[Response]]] = Future {
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