package stretchypants

import com.ning.http.client.AsyncHandler
import dispatch.{ url, Req, Http }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.Printer.compact
import org.json4s.native.JsonMethods.render

case class Script(
  src: String,
  params: Map[String, String] = Map.empty[String, String],
  lang: Option[String] = None)

object Client {
  type Handler[T] = AsyncHandler[T]
  val Agent = "StretchyPants/0.1.0-SNAPSHOT"
  trait Completion {
    def apply[T](handler: Client.Handler[T])(implicit ec: ExecutionContext): Future[T]
  }
}

case class Client(
  host: String = "http://0.0.0.0:9200", http: Http = Http) {
  private[this] def root = url(host)

  def request[T]
    (req: Req)
    (handler: Client.Handler[T])
    (implicit ec: ExecutionContext): Future[T] =
     http(req <:< Map("User-Agent" -> Client.Agent) > handler)

  def complete(req: Req): Client.Completion = new Client.Completion {
    override def apply[T](handler: Client.Handler[T])(implicit ec: ExecutionContext) =
      request(req)(handler)
  }

  def refresh() = complete(root.POST / "_refresh")

  def status(index: String*) =
    complete(
      if (index.nonEmpty) root / index.mkString(",") / "_status"
      else root / "_status")

  case class Mapping(
    _index: List[String] = Nil,
    _kind: List[String] = Nil) {
    def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) = {
      val endpoint = (_index, _kind) match {
        case (Nil, Nil) => root / "_all"
        case (Nil, kinds) => root / "_all" / kinds.mkString(",")
        case (indexes, Nil) => root / indexes.mkString(",")
        case (indexes, kinds) => root / indexes.mkString(",") / kinds.mkString(",")
      }
      request(endpoint / "_mapping")(hand)
    }
  }
  def mapping = Mapping()

  case class Indexer(
    index: String, kind: String,
    _doc: Option[String]       = None,
    _id: Option[String]        = None,
    _version: Option[Int]      = None,
    _routing: Option[String]   = None,
    _parent: Option[String]    = None,
    _timestamp: Option[String] = None,
    _ttl: Option[FiniteDuration]      = None,
    _consistency: Option[Consistency] = None,
    _timeout: Option[FiniteDuration] = None,
    _replication: Option[Replication] = None,
    _refresh: Boolean          = false,
    _create: Boolean           = false) {
    
    def doc(d: String) = copy(_doc = Some(d))
    def id(i: String)  = copy(_id = Some(i))
    def version(v: Int) = copy(_version = Some(v))
    def routing(r: String) = copy(_routing = Some(r))
    def parent(p: String) = copy(_parent = Some(p))
    def timestamp(s: String) = copy(_timestamp = Some(s))
    def ttl(t: FiniteDuration) = copy(_ttl = Some(t))
    def consistency(c: Consistency) = copy(_consistency = Some(c))
    def timeout(to: FiniteDuration) = copy(_timeout = Some(to))
    def replication(r: Replication) = copy(_replication = Some(r))
    def refresh(r: Boolean) = copy(_refresh = r)
    def create(c: Boolean) = copy(_create = c)

    def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) =
      _doc.map( doc => 
      request(_id.map(root / index / kind / _).getOrElse(root / index / kind).PUT
              <<? Map.empty[String, String]  ++
                _version.map("version" -> _.toString) ++
                Some("create").filter(Function.const(_create)).map("op_type" -> _) ++
                _routing.map("routing" -> _) ++
                _parent.map("parent" -> _) ++
                _timestamp.map("timestamp" -> _) ++
                _ttl.map("ttl" -> _.length.toString) ++
                _consistency.map("consistency" -> _.value) ++
                _replication.map("replication" -> _.value) ++
                Some("true").filter(Function.const(_refresh)).map("refresh" -> _) ++
                _timeout.map("timeout" -> _.length.toString)
              << doc)(hand)).getOrElse(
        Future.failed(new IllegalArgumentException("doc is required"))
      )
  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html */
  def index(index: String, kind: String) = Indexer(index, kind).doc(_)

  case class Get(
    index: String, kind: String,
    _id: Option[String] = None,
    _realtime: Boolean       = true,
    _source: Boolean         = true,
    _include: Option[String] = None,
    _exclude: Option[String] = None,
    _routing: Option[String] = None,
    _preference: Option[Preference] = None,
    _refresh: Boolean        = false,
    _fields: Seq[String]     = Seq.empty[String]) {

    def id(i: String) = copy(_id = Some(i))
    def realtime(rt: Boolean) = copy(_realtime = rt)
    def source(s: Boolean) = copy(_source = s)
    def include(incl: String) = copy(_include = Some(incl))
    def exclude(excl: String) = copy(_exclude = Some(excl))
    def routing(r: String) = copy(_routing = Some(r))
    def preference(p: Preference) = copy(_preference = Some(p))
    def refresh(r: Boolean) = copy(_refresh = r)
    def fields(fs: String*) = copy(_fields = fs.toList)

    def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) =
      _id.map( id =>
        request(root / index / kind / id <<? Map.empty[String, String] ++
                Some("false").filter(Function.const(!_realtime)).map("realtime" -> _) ++
                Some("false").filter(Function.const(!_source)).map("_source" -> _) ++
                _include.map("_source_include" -> _) ++
                _exclude.map("_source_exclude" -> _) ++
                Some(_fields).filter(_.nonEmpty).map("fields" -> _.mkString(",")) ++
                _routing.map("routing" -> _) ++
                _preference.map("preference" -> _.value) ++
                Some("true").filter(Function.const(_refresh)).map("refresh" -> _))(hand)).getOrElse(
                  Future.failed(new IllegalArgumentException("id is required"))
                )

  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html */
  def get(index: String, kind: String) = Get(index, kind).id(_)

  object MultiGet {
    case class Doc(
      id: String,
      _index: Option[String]        = None,
      _kind: Option[String]         = None,
      _source: Option[Source]       = None,
      _fields: Option[List[String]] = None) {
      def index(i: String) = copy(_index = Some(i))
      def kind(k: String) = copy(_kind = Some(k))
      def source(src: Source) = copy(_source = Some(src))
      def fields(fs: String*) = copy(_fields = Some(fs.toList))
      def asJson =
        ("id"      -> id) ~
        ("_index"  -> _index) ~
        ("_type"   -> _kind) ~
        ("_source" -> _source.map(_.asJson))
    }

    def doc(id: String) = Doc(id)
  }

  case class MultiGet(
    _index: Option[String]    = None,
    _kind: Option[String]     = None,
    _docs: List[MultiGet.Doc] = Nil,
    _ids: List[String]        = Nil) {

    def index(i: String) = copy(_index = Some(i))
    def kind(k: String) = copy(_kind = Some(k))
    def docs(ds: MultiGet.Doc*) = copy(_docs = ds.toList)
    def ids(ids: String*) = copy(_ids = ids.toList)

    def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) = {
      val endpoint = (_index, _kind) match {
        case (None, None)              => root
        case (None, Some(kind))        => root / "_all" / kind // don't think this is right
        case (Some(index), None)       => root / index
        case (Some(index), Some(kind)) => root / index / kind
      }
      request(endpoint / "_mget" << compact(render(
        ("docs" -> Some(_docs).filter(_.nonEmpty).map(_.map(_.asJson))) ~
        ("ids" -> Some(_ids).filter(_.nonEmpty)))))(hand)
    }
  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-multi-get.html. */
  def multiGet = MultiGet()

  case class Delete(
    index: String, kind: String,
    _id: Option[String]      = None,
    _version: Option[String] = None,
    _routing: Option[String] = None,
    _parent: Option[String]  = None,
    _replication: Option[Replication] = None,
    _consistency: Option[Consistency] = None,
    _refresh: Boolean = false,
    _timeout: Option[FiniteDuration] = None) {

    def id(i: String) = copy(_id = Some(i))
    def version(v: String) = copy(_version = Some(v))
    def routing(r: String) = copy(_routing = Some(r))
    def parent(p: String) = copy(_parent = Some(p))
    def replication(r: Replication) = copy(_replication = Some(r))
    def consistency(c: Consistency) = copy(_consistency = Some(c))
    def refresh(r: Boolean) = copy(_refresh = r)
    def timeout(to: FiniteDuration) = copy(_timeout = Some(to))

    def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) =
      _id.map( id =>
        request(root.DELETE / index / kind / id <<? Map.empty[String, String] ++
         _version.map("version" -> _) ++
         _routing.map("routing" -> _) ++
         _parent.map("_parent" -> _) ++
         _replication.map("replication" -> _.value) ++
         _consistency.map("consistency" -> _.value) ++
         Some("false").filter(Function.const(!_refresh)).map("refresh" -> _) ++
         _timeout.map("timeout" -> _.length.toString))(hand)).getOrElse(
           Future.failed(new IllegalArgumentException("id is required"))
         )
  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-delete.html */
  def delete(index: String, kind: String) = Delete(index, kind).id(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-update.html */
  def update(
    index: String, kind: String)
    (id: String,
     script: Option[Script] = None) =
    (root.POST / index / kind / id / "_update"
     << compact(render(
       script.map( s => ("script" -> s.src) ~ ("params" -> s.params)).getOrElse(JNothing))))

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html */
  // todo

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


  case class URISearch
    (_q: Option[String]               = None,
     _index: List[String]             = Nil,
     _kind: List[String]              = Nil,
     _defaultField: Option[String]    = None,
     _analyzer: Option[String]        = None,
     _defaultOperator: Option[String] = None,
     _explain: Option[Boolean]        = None,
     _source: Option[Boolean]         = None,
     _sourceInclude: Option[String]   = None,
     _sourceExclude: Option[String]   = None,
     _fields: Seq[String]             = Seq.empty[String],
     _sort: List[String]              = Nil,
     _trackScores: Option[Boolean]    = None,
     _timeout: Option[FiniteDuration] = None,
     _from: Option[Long]              = None,
     _size: Option[Int]               = None,
     _searchType: Option[SearchType]  = None,
     _lowercaseExpandedTerms: Option[Boolean] = None,
     _analyzeWildcard: Option[Boolean] = None,
     _routing: Option[String]         = None,
     _stats: List[String]             = List.empty[String]) {

      def query(q: String) = copy(_q = Some(q))
      def index(idx: String*) = copy(_index = idx.toList)
      def kind(types: String*) = copy(_kind = types.toList)
      def defaultField(df: String) = copy(_defaultField = Some(df))
      def analyzer(a: String) = copy(_analyzer = Some(a))
      def defaultOperator(defOp: String) = copy(_defaultOperator = Some(defOp))
      def explain(exp: Boolean) = copy(_explain = Some(exp))
      def source(src: Boolean) = copy(_source = Some(src))
      def sourceInclude(incl: String) = copy(_sourceInclude = Some(incl))
      def sourceExclude(excl: String) = copy(_sourceExclude = Some(excl))
      def fields(fs: String*) = copy(_fields = fs.toList)
      def sort(s: String*) = copy(_sort = s.toList)
      def trackStores(t: Boolean) = copy(_trackScores = Some(t))
      def timeout(to: FiniteDuration) = copy(_timeout = Some(to))
      def from(f: Long) = copy(_from = Some(f))
      def size(s: Int) = copy(_size = Some(s))
      def searchType(tpe: SearchType) = copy(_searchType = Some(tpe))
      def lowercaseExpandedTerms(exp: Boolean) = copy(_lowercaseExpandedTerms = Some(exp))
      def analyzeWildcard(a: Boolean) = copy(_analyzeWildcard = Some(a))
      def stats(s: String*) = copy(_stats = s.toList)

      def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) =
        _q.map { q => 
          val endpoint = (_index, _kind) match {
            case (Nil, Nil) => root / "_all"
            case (Nil, kinds) => root / "_all" / kinds.mkString(",")
            case (indexes, Nil) => root / indexes.mkString(",")
            case (indexes, kinds) => root / indexes.mkString(",") / kinds.mkString(",")
          }
          request(endpoint / "_search"
                  <<? Map.empty[String, String] ++
                    _q.map("q" -> _) ++
                    _defaultField.map("df" -> _) ++
                    _analyzer.map("analyzer" -> _) ++
                    _defaultOperator.map("default_operator" -> _) ++
                    _explain.map("explain" -> _.toString) ++
                    _source.map("_source" -> _.toString) ++
                    _sourceInclude.map("_source_include" -> _) ++
                    _sourceExclude.map("_source_exclude" -> _) ++
                    Some(_fields).filter(_.nonEmpty).map("fields" -> _.mkString(",")) ++
                    Some(_sort).filter(_.nonEmpty).map("sort" -> _.mkString(",")) ++
                    _trackScores.map("track_scores" -> _.toString) ++
                    _timeout.map("timeout" -> _.length.toString) ++
                    _from.map("from" -> _.toString) ++
                    _size.map("size" -> _.toString) ++
                    _searchType.map("search_type" -> _.value) ++
                    _lowercaseExpandedTerms.map("lowercase_expanded_terms" -> _.toString) ++
                    _analyzeWildcard.map("analyze_wildcard" -> _.toString) ++
                    _routing.map("routing" -> _))(hand)
              }.getOrElse(
                Future.failed(new IllegalArgumentException("id is required"))
              )
    }

  /** query is expected to be in the form field:value or just value with df=field */
  def uriSearch = URISearch().query(_)

  case class Search
    (_index: String,
     _kind: String,
     _term: Option[(String, String)]  = None,
     _timeout: Option[FiniteDuration] = None,
     _from: Option[Long]              = None,
     _size: Option[Int]               = None,
     _searchType: Option[SearchType]  = None,
     _sort: List[Sort]                 = Nil,
     _trackScores: Option[Boolean]    = None,
     // todo partial source
     _source: Option[Source]         = None,
     // todo script fields
     // todo fielddata_fields
     _fields: Option[Seq[String]]     = None,
     _postFilter: Option[(String, String)] = None,
     _facets: Option[List[(String, Facet)]] = None) {

     def index(i: String) = copy(_index = i)
     def kind(k: String) = copy(_kind = k)
     def term(t: (String, String)) = copy(_term = Some(t))
     def timeout(to: FiniteDuration) = copy(_timeout = Some(to))
     def from(f: Long) = copy(_from = Some(f))
     def size(s: Int) = copy(_size = Some(s))
     def searchType(typ: SearchType) = copy(_searchType = Some(typ))
     def sort(sorts: Sort*) = copy(_sort = sorts.toList)
     def trackScores(t: Boolean) = copy(_trackScores = Some(t))
     def source(src: Source) = copy(_source = Some(src))
     def fields(fs: String*) = copy(_fields = Some(fs.toList))
     def postFilter(filt: (String, String)) = copy(_postFilter = Some(filt))
     def facets(fs: (String, Facet)*) = copy(_facets = Some(fs.toList))

     def apply[T](hand: Client.Handler[T])(implicit ec: ExecutionContext) =
       _term.map {
         case (field, value) =>
           val body = compact(render(
                     ("fields"  -> _fields) ~
                     ("_source" -> _source.map(_.asJson)) ~
                     ("from"    -> _from) ~
                     ("size"    -> _size) ~
                     ("track_scores" -> _trackScores) ~
                     ("sort" -> Some(_sort).filter(_.nonEmpty).map {
                       _.map { sort =>
                         (sort.field -> ("order" -> sort.order.map(_.value)))
                       }
                     }) ~
                     ("query"       -> ("term" -> (field -> value))) ~
                     ("post_filter" -> _postFilter.map { pf =>
                       ("term" -> (pf._1 -> pf._2))
                     }) ~
                     ("facets" -> _facets.map { fs =>
                       (JObject() /: fs) {
                         case (fobj, (name, facet)) =>
                           fobj ~ (name -> facet.asJson)
                       }
                     })))
           println("body %s" format body)
           request(root.POST / _index / _kind / "_search"
                   <<? Map.empty[String, String] ++
                   _timeout.map("timeout"        -> _.length.toString) ++
                   _searchType.map("search_type" -> _.value)
                   << body)(hand)
       }.getOrElse(
         Future.failed(new IllegalArgumentException("term is required"))
       )
  }

  def search(index: String, kind: String) = Search(index, kind).term(_)
}
