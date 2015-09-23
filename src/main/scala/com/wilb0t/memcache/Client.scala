package com.wilb0t.memcache

import java.net.{Socket, InetAddress}
import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait Client {
  def execute(command:Command)(implicit timeout: Duration) : Future[Response]
  def getM(gets: List[Command.Get])(implicit timeout: Duration) : Future[List[Response]]
  def setM(sets: List[Command.Setter])(implicit timeout: Duration) : Future[List[Response]]
  def delM(dels: List[Command.Delete])(implicit timeout: Duration) : Future[List[Response]]
  def incDecM(incDecs: List[Command.IncDec])(implicit timeout: Duration) : Future[List[Response]]
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

        override def execute(command: Command)(implicit timeout: Duration): Future[Response] = Future {
          out.write(command.serialize)
          out.flush()
          Response.Parser(in)(timeout)._2
        }

        def executeM(commands: List[Command])(implicit timeout: Duration): Future[List[Response]] =
          Future {
            val quietCmds = commands.zipWithIndex.map { case (cmd, i) => Command.quietCommand(cmd, i) }
            val quietSize = quietCmds.size
            quietCmds.foreach { cmd => out.write(cmd.serialize) }
            //the Noop triggers any responses for the quiet commands
            out.write(Command.Noop(quietSize).serialize)
            out.flush()
            val responses = Response.Parser(in, quietSize)(timeout)
            quietCmds.map { cmd => responses.getOrElse(cmd.opaque, Command.defaultResponse(cmd)) }
          }

        override def getM(gets: List[Command.Get])(implicit timeout: Duration) = executeM(gets)

        override def setM(sets: List[Command.Setter])(implicit timeout: Duration) = executeM(sets)

        override def delM(dels: List[Command.Delete])(implicit timeout: Duration) = executeM(dels)

        override def incDecM(incDecs: List[Command.IncDec])(implicit timeout: Duration) = executeM(incDecs)

        override def close(): Unit = {
          in.close()
          out.close()
          socket.close()
        }
      }
    )
  }
}
