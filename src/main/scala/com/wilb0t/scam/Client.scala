package com.wilb0t.scam

import java.io.{IOException, OutputStream, InputStream}
import java.net.{SocketException, ConnectException, Socket, InetAddress}
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}

trait Client {
  def execute(command:Command)(implicit timeout: Duration) : Future[Response]
  def getM(gets: List[Command.Get])(implicit timeout: Duration) : Future[List[Response]]
  def setM(sets: List[Command.Setter])(implicit timeout: Duration) : Future[List[Response]]
  def delM(dels: List[Command.Delete])(implicit timeout: Duration) : Future[List[Response]]
  def incDecM(incDecs: List[Command.IncDec])(implicit timeout: Duration) : Future[List[Response]]
  def close(): Unit
}

protected[scam] trait Connection {
  def streams(): Try[(InputStream, OutputStream)]
  def close(): Unit
}

protected[scam] class ClientImpl(conn: Connection, responseReader: Response.Reader) extends Client {
  val exCtx = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  override def execute(command: Command)(implicit timeout: Duration): Future[Response] =
    Future {
      val (in, out) = conn.streams().get
      out.write(command)
      out.flush()
      responseReader.read(in, command)(timeout)
    }(exCtx)

  def executeM(commands: List[Command])(implicit timeout: Duration): Future[List[Response]] =
    Future {
      val (in, out) = conn.streams().get
      val quietCmds = commands.zipWithIndex.map{ case (cmd, i) => (Command.quietCommand(cmd, i), i) }
      val quietCmdsMap = quietCmds.foldLeft(Map[Int,Command]()) { case (map, (cmd, i)) => map + ((i, cmd)) }
      val finalCommandTag = quietCmds.size
      quietCmds.foreach { case (cmd, i) => out.write(cmd) }
      //the Noop triggers any responses for the quiet commands and guarantees at least 1 response
        //so the response parser doesn't time out
      out.write(Command.Noop(finalCommandTag))
      out.flush()
      val responses = responseReader.read(in, finalCommandTag, quietCmdsMap)(timeout)
      quietCmds.map { case (cmd, _) => responses.getOrElse(cmd.opaque, Command.defaultResponse(cmd)) }
    }(exCtx)

  override def getM(gets: List[Command.Get])(implicit timeout: Duration) = executeM(gets)

  override def setM(sets: List[Command.Setter])(implicit timeout: Duration) = executeM(sets)

  override def delM(dels: List[Command.Delete])(implicit timeout: Duration) = executeM(dels)

  override def incDecM(incDecs: List[Command.IncDec])(implicit timeout: Duration) = executeM(incDecs)

  override def close() = conn.close()
}

protected[scam] class ConnectionImpl(address: InetAddress, port: Int, retries: Int) extends Connection {
  private var socketTry: Try[Socket] = Failure(new ConnectException("Not initially connected"))

  @tailrec
  private def getSocket(_retries: Int): Try[Socket] =
    (socketTry, _retries) match {
      case (Success(s), r) =>
        if (s.isConnected) {
          Success(s)
        } else {
          socketTry = Failure(new SocketException("Socket disconnected"))
          getSocket(r)
        }

      case (f, r) if r > 0 =>
        socketTry = Try{ new Socket(address, port) }
        getSocket(r - 1)

      case (Failure(e), _) =>
        Failure(new IOException("Failed to connect after " + retries + " retries", e))
    }

  override def streams() =
    getSocket(retries).map{ s => (s.getInputStream, s.getOutputStream) }

  override def close() = socketTry.foreach{ _.close() }
}

object Client {
  def defaultConnectionRetries: Int = 3

  def apply(address: InetAddress, port: Int, connectRetries: Int = defaultConnectionRetries): Client =
    new ClientImpl(
      new ConnectionImpl(address, port, connectRetries),
      Response.Reader()
    )
}
