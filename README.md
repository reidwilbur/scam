# SCAM
## A pure scala memcached client

SCAM is a memcached client that intends to have no external dependencies outside
of the standard scala runtime (2.11.4).

SCAM uses the memcached binary protocol for transport and offers batched
command execution to take advantage of the binary protocol's quiet 
commands.

Example usage below

```
import scala.concurrent.ExecutionContext.Implicits.global
import com.wilb0t.scam.{Client, Command, Response}
import scala.util.Try
import scala.concurrent.{Future, Await}

val address = InetAddress.getByName("192.168.59.103")

val client: Client = Client(address, 11211)
val response: Future[Response] = 
    client.execute(
        Command.Set("somekey", 0, 3600, None, Array[Byte](0x0, 0x1, 0x2, 0x3))
    )(Duration(250, TimeUnit.MILLISECONDS))

println(Await.result(responseF, Duration(1, TimeUnit.SECONDS)))

val responseM : Future[Response] = client.getM(List(
    Command.Get("somekey4"),
    Command.Get("somekey2"),
    Command.Get("somekey3"),
    Command.Get("somekey1")
))(Duration(250, TimeUnit.MILLISECONDS))

println(Await.result(responseM, Duration(1, TimeUnit.SECONDS)))
```
