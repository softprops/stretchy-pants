package stretchypants

import dispatch._, dispatch.Defaults._
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{ compact, render }
import org.scalatest.FunSpec 
import stretchypants._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

class ClientSpec extends FunSpec {
  describe("Client") {

    it ("should provide doc interfaces") {
      val es = Client()
      val tweeter = es.index("twitter", "tweet")
      val tweeting = es.get("twitter", "tweet")
      val detweet = es.delete("twitter", "tweeti")
      val f = for {
        put <- tweeter(str(
          ("content" -> "stretchy pants!") ~
          ("user" -> ("name" -> "@stretchy")))).id("1").timeout(1 second)(as.String)
        get <- tweeting("1")(as.String)
        del <- detweet("1").timeout(1 second)(as.String)
      } yield {
        println(put)
        println(get)
        println(del)
      }
      Await.ready(f, Duration.Inf)
    }
    
  }

  def str(j: JValue) = compact(render(j))
}
