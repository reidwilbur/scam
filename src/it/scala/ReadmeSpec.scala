import org.scalatest.FunSpec

class ReadmeSpec extends FunSpec {
  describe("Scam client") {
    it("example usage") {
      import java.net.InetAddress
      import java.util.concurrent.TimeUnit

      import com.wilb0t.scam.{Client, Command, Response}

      import scala.concurrent.duration.Duration
      import scala.concurrent.{Await, Future}

      val address = InetAddress.getByName("192.168.59.103")

      val client: Client = Client(address, 11211)
      val response: Future[Response] =
        client.execute(
          Command.Set("somekey", 0, 3600, None, Array[Byte](0x0, 0x1, 0x2, 0x3))
        )(Duration(250, TimeUnit.MILLISECONDS))

      println(Await.result(response, Duration(1, TimeUnit.SECONDS)))

      val responseM: Future[List[Response]] = client.getM(List(
        Command.Get("somekey4"),
        Command.Get("somekey2"),
        Command.Get("somekey3"),
        Command.Get("somekey1")
      ))(Duration(250, TimeUnit.MILLISECONDS))

      println(Await.result(responseM, Duration(1, TimeUnit.SECONDS)))
    }
  }
}
