package stretchypants

import dispatch.url
import scala.concurrent.duration.FiniteDuration
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.Printer.compact
import org.json4s.native.JsonMethods.render

case class Script(
  src: String,
  params: Map[String, String] = Map.empty[String, String],
  lang: Option[String] = None)

/*sealed trait Action
object Action {
  abstract class Value(
    def name: String
  )
  case class Index(
    index: String,
    kind: String,
    doc: String,
    id: Option[String] = None,
    refresh: Option[Boolean] = None,
    consistency: Option[Consistency] = None,
    ttl: Option[FiniteDuration] = None,
    timestamp: Option[Long] = None,
    parent: Option[String]  = None,
    routing: Option[String] = None,
    version: Option[Int]    = None) extends Value("index")
  case class Create(
    index: String,
    kind: String,
    doc: String,
    id: Option[String] = None,
    refresh: Option[Boolean] = None,
    consistency: Option[Consistency] = None,
    ttl: Option[FiniteDuration] = None,
    timestamp: Option[Long] = None,
    parent: Option[String]  = None,
    routing: Option[String] = None,
    version: Option[Int]    = None) extends Value("create")
  case class Delete(
    index: String,
    kind: String,
    id: String,
    refresh: Option[Boolean] = None,
    consistency: Option[Consistency] = None,
    ttl: Option[FiniteDuration] = None,
    timestamp: Option[Long] = None,
    parent: Option[String]  = None,
    routing: Option[String] = None,
    version: Option[Int]    = None) extends Value("delete")
  case class Update(
    index: String,
    kind: String,
    id: String,
    doc: String,
    upsert: Option[String] = None,
    docAsUpsert: Option[Boolean] = None,
    script: Option[Script] = None,
    retryOnConflict: Option[Int] = None,
    refresh: Option[Boolean] = None,
    consistency: Option[Consistency] = None,
    ttl: Option[FiniteDuration] = None,
    timestamp: Option[Long] = None,
    parent: Option[String]  = None,
    routing: Option[String] = None,
    version: Option[Int]    = None) extends Value("update")
}
*/

case class Client(host: String) {
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
     replication: Option[Replication] = None,
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
          replication.map("replication" -> _.value) ++
          Some("true").filter(Function.const(refresh)).map("refresh" -> _) ++
          timeout.map("timeout" -> _.length.toString)
      << doc)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html */
  def get(
    index: String,
    kind: String)
    (id: String,
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-multi-get.html.
   *  todo: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-multi-get.html#mget-source-filtering
   */
  def multiGet(index: String, kind: String)(ids: String*) =
    (root.POST / index / kind / "_mget" << compact(render(("ids" -> ids.toList))))

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-delete.html */
  def delete(
    index: String,
    kind: String)
    (id: String,
     version: Option[String] = None,
     routing: Option[String] = None,
     parent: Option[String] = None,
     replication: Option[Replication] = None,
     consistency: Option[Consistency] = None,
     refresh: Boolean = false,
     timeout: Option[FiniteDuration] = None) =
    (root.DELETE / index / kind / id <<? Map.empty[String, String] ++
     version.map("version" -> _) ++
     routing.map("routing" -> _) ++
     parent.map("_parent" -> _) ++
     replication.map("replication" -> _.value) ++
     consistency.map("consistency" -> _.value) ++
     Some("false").filter(Function.const(!refresh)).map("refresh" -> _) ++
     timeout.map("timeout" -> _.length.toString))

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-update.html */
  def update(
    index: String, kind: String)
    (id: String,
     script: Option[Script] = None) =
    (root.POST / index / kind / id / "_update"
     << compact(render(
       script.map( s => ("script" -> s.src) ~ ("params" -> s.params)).getOrElse(JNothing))))

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html */
  /*def bulk(index: String = "", kind: String = "")(actions: Action*) = {
    val endpoint = (root.POST /: index :: kind :: Nil) {
      case (u, s) => if (u.isEmpty || kind.isEmpty) u else u / s
    }
    (endpoint / "_bulk" << actions.map {
      case i: Index =>
        compact(render(
          (i.name -> ())))
    }.mkString("\n"))
  }*/

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-delete-by-query.html */
  def deleteQuery(index: String*)(kind: String*)(
    q: String,
    defaultField: Option[String]    = None,
    analyzer: Option[String]        = None,
    defaultOperator: Option[String] = None) =
    (root.DELETE / (if (index.isEmpty) "_all" else index.mkString(",")) /
     (if (kind.isEmpty) "_all" else kind.mkString(",")) / "_query"
     <<? Map("q" -> q) ++ defaultField.map("df" -> _) ++
           analyzer.map("analyzer" -> _) ++
           defaultOperator.map("default_operator" -> _))

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-termvectors.html */
  def termVector(index: String, kind: String)(id: String)(
    fields: Seq[String] = Seq.empty[String],
    termStats: Boolean = false,
    fieldStats: Boolean = false,
    offsets: Boolean = false,
    payloads: Boolean = false,
    positions: Boolean = false) =
    (root / index / kind / id <<? Map.empty[String, String] ++
     Some(fields).filter(_.nonEmpty).map("fields" -> _.mkString(",")) ++
     Some("true").filter(Function.const(termStats)).map("term_statistics" -> _) ++
     Some("true").filter(Function.const(fieldStats)).map("field_statistics" -> _) ++
     Some("true").filter(Function.const(offsets)).map("offsets" -> _) ++
     Some("true").filter(Function.const(payloads)).map("payloads" -> _) ++
     Some("true").filter(Function.const(positions)).map("positions" -> _))

   /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-multi-termvectors.html */
   //def multiTermVector(docs: Seq[TermVector]) =
   //  (root.POST / "_mtermvectors" << compact(render("docs" -> docs.map())))

   def uriSearch
     (index: String*)
     (kind: String*)
     (q: String,
      defaultField: Option[String]    = None,
      analyzer: Option[String]        = None,
      defaultOperator: Option[String] = None,
      explain: Option[Boolean]        = None,
      source: Option[Boolean]         = None,
      sourceInclude: Option[String]   = None,
      sourceExclude: Option[String]   = None,
      fields: Seq[String]             = Seq.empty[String],
      sort: Seq[String]               = Seq.empty[String],
      trackScores: Option[Boolean]    = None,
      timeout: Option[FiniteDuration] = None,
      from: Option[Long]              = None,
      size: Option[Int]               = None,
      searchType: Option[SearchType]  = None,
      lowercaseExpandedTerms: Option[Boolean] = None,
      analyzeWildcard: Option[Boolean]= None,
      routing: Option[String]         = None,
      stats: Seq[String]              = Seq.empty[String]) = {
      val endpoint = (root /: Seq(index, kind)) {
        case (ep, seg) =>
          ep / (if (seg.isEmpty) "_all" else seg.mkString(","))
      }
     (endpoint / "_search"
      <<? Map("q" -> q) ++
        defaultField.map("df" -> _) ++
        analyzer.map("analyzer" -> _) ++
        defaultOperator.map("default_operator" -> _) ++
        explain.map("explain" -> _.toString) ++
        source.map("_source" -> _.toString) ++
        sourceInclude.map("_source_include" -> _) ++
        sourceExclude.map("_source_exclude" -> _) ++
        Some(fields).filter(_.nonEmpty).map("fields" -> _.mkString(",")) ++
        Some(sort).filter(_.nonEmpty).map("sort" -> _.mkString(",")) ++
        trackScores.map("track_scores" -> _.toString) ++
        timeout.map("timeout" -> _.length.toString) ++
        from.map("from" -> _.toString) ++
        size.map("size" -> _.toString) ++
        searchType.map("search_type" -> _.value) ++
        lowercaseExpandedTerms.map("lowercase_expanded_terms" -> _.toString) ++
        analyzeWildcard.map("analyze_wildcard" -> _.toString) ++
        routing.map("routing" -> _)
        )
     }
}
