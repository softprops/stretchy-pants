package stretchypants

import dispatch.url
import scala.concurrent.duration.FiniteDuration

case class Client(host: String) {
  sealed trait Consistency {
    def value: String
  }
  object Consistency {
    abstract class Value(val value: String) extends Consistency
    case object One extends Value("one")
    case object Quorum extends Value("quorum")
    case object All extends Value("all")
  }

  private[this] def root = url(host)

  // { "_index": kind, "_type": "kind", "_id": id, "_version": <int>, "created": <bool> }
  def index(
    index: String, kind: String)
    (doc: String,
     version: Option[Int]      = None,
     id: Option[String]        = None,
     routing: Option[String]   = None,
     parent: Option[String]    = None,
     timestamp: Option[String] = None,
     ttl: Option[FiniteDuration]      = None,
     consistency: Option[Consistency] = None,
     timeout: Option[FiniteDuration] = None,
     asyncReplication: Boolean = false,
     refresh: Boolean          = false,
     create: Boolean           = false) =
    (id.map(root / kind / _).getOrElse(root / kind).PUT
      <<? Map.empty[String, String]  ++
          version.map("version" -> _.toString) ++
          Some("create").filter(Function.const(create)).map("op_type" -> _) ++
          routing.map("routing" -> _) ++
          parent.map("parent" -> _) ++
          timestamp.map("timestamp" -> _) ++
          ttl.map("ttl" -> _.length.toString) ++
          consistency.map("consistency" -> _.value) ++
          Some("async").filter(Function.const(asyncReplication)).map("replication" -> _) ++
          Some("true").filter(Function.const(refresh)).map("refresh" -> _) ++
          timeout.map("timeout" -> _.length.toString)
      << doc)
}
