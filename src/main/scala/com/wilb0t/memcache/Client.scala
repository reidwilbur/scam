package com.wilb0t.memcache

import java.net.{Socket, InetAddress}
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait Client {
  def execute(command:Command) : Future[Response]
  def getM(gets: List[Command.Get]) : Future[List[Response]]
  def setM(sets: List[Command.Set]) : Future[List[Response]]
  def delM(dels: List[Command.Delete]) : Future[List[Response]]
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

        def executeM(commands: List[Command]): Future[List[Response]] =
          Future {
            val quietCmds = commands.zipWithIndex.map { case (cmd, i) => Command.quiet(cmd, i) }
            val quietSize = quietCmds.size
            quietCmds.foreach { cmd => out.write(cmd.serialize) }
            //the Noop triggers any responses for the quiet commands
            out.write(Command.Noop(quietSize).serialize)
            out.flush()
            val responses = Response.Parser(in, quietSize)
            quietCmds.map { cmd => responses.getOrElse(cmd.opaque, Response.default(cmd)) }
          }

        override def getM(gets: List[Command.Get]) = executeM(gets)

        override def setM(sets: List[Command.Set]) = executeM(sets)

        override def delM(dels: List[Command.Delete]) = executeM(dels)

        override def close(): Unit = {
          in.close()
          out.close()
          socket.close()
        }
      }
    )
  }
}
