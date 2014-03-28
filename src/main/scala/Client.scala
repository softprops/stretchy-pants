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

  sealed trait Preference {
    def value: String
  }
  object Preference {
    abstract class Value(val value: String) extends Preference
    case object Primary extends Value("_primary")
    case object Local extends Value("_local")
    case class Custom(val value: String) extends Preference
  }

  private[this] def root = url(host)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html */
  def get(
    index: String,
    kind: String,
    id: String,
    realtime: Boolean       = true,
    source: Boolean         = true,
    include: Option[String] = None,
    exclude: Option[String] = None,
    routing: Option[String] = None,
    preference: Option[Preference] = None,
    refresh: Boolean        = false,
    fields: Seq[String]     = Seq.empty[String]) =
    (root / index / kind / id <<? Map.empty[String, String] ++
     Some("false").filter(Function.const(!realtime)).map("realtime" -> _) ++
     Some("false").filter(Function.const(!source)).map("_source" -> _) ++
     include.map("_source_include" -> _) ++
     exclude.map("_source_exclude" -> _) ++
     Some(fields).filter(_.nonEmpty).map("fields" -> _.mkString(",")) ++
     routing.map("routing" -> _) ++
     preference.map("preference" -> _.value) ++
     Some("true").filter(Function.const(refresh)).map("refresh" -> _))
}
