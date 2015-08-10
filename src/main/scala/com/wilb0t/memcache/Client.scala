package com.wilb0t.memcache

import java.net.{Socket, InetAddress}
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try}

trait Client {
  def execute(command:Command) : Future[Response]
  def getM(gets: List[Command.Get]) : Future[List[Response]]
  def setM(sets: List[Command.Set]) : Future[List[Response]]
  def delM(sets: List[Command.Delete]) : Future[List[Response]]
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

        def executeM[T <: Command, QT <: Command]
          (commands: List[T])(toQuiet: (T, Int) => QT)(defaultResponse: QT => Response)
        : Future[List[Response]] =
          Future {
            val quietCmds = commands.zipWithIndex.map { case (cmd, i) => toQuiet(cmd, i) }
            val quietSize = quietCmds.size
            quietCmds.foreach { cmd => out.write(cmd.serialize) }
            //the Noop triggers any responses for the quiet commands
            out.write(Command.Noop(quietSize).serialize)
            out.flush()
            val responses = Response.Parser(in, quietSize)
            quietCmds.map { cmd => responses.getOrElse(cmd.opaque, defaultResponse(cmd)) }
          }

        override def getM(gets: List[Command.Get]): Future[List[Response]] =
          executeM(gets)
            { case (g, i) => Command.GetQ(g.getkey, i) }
            { case _ => Response.KeyNotFound() }

        override def setM(sets: List[Command.Set]): Future[List[Response]] =
          executeM(sets)
            { case (s, i) => Command.SetQ(s.setkey, s.flags, s.exptime, i, s.cas, s.setvalue) }
            { case sq => Response.Success(Some(sq.setkey), 0x0, sq.value) }

        override def delM(sets: List[Command.Delete]): Future[List[Response]] =
          executeM(sets)
            { case (d, i) => Command.DeleteQ(d.delkey, i) }
            { case dq => Response.Success(Some(dq.delkey), 0x0, None) }

        override def close(): Unit = {
          in.close()
          out.close()
          socket.close()
        }
      }
    )
  }
}
