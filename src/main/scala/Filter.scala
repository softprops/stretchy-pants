package stretchypants

import org.json4s.JsonDSL._
import org.json4s.{ JObject, JValue }

trait Filter {
  def asJson: JValue
}

/* http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-filters.html */
object Filter {
  case class And(
    q: Query, ands: List[Query] = Nil,
    _cache: Option[Boolean] = None) extends Filter {
    def and(a: Query) = copy(ands = a :: ands)
    def cache(c: Boolean) = copy(_cache = Some(c))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("and" ->
         ("_cache" -> _cache) ~
         ("filters" -> ands.map(_.asJson)))))
  }

  def and(primary: Query) = And(primary)

  case class Bool(
    q: Query, clauses: Map[String, List[Query]]
      = Map.empty[String, List[Query]].withDefaultValue(Nil),
    _cache: Option[Boolean] = None) extends Filter {
    def must = clause("must")_
    def mustNot = clause("must_not")_
    def should = clause("should")_
    def cache(c: Boolean) = copy(_cache = Some(c))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("bool" -> (JObject() /: clauses) {
          case (obj, (k, xs)) =>
            obj ~ (k, xs.map(_.asJson))
        }) ~
        ("_cache" -> _cache)))
    private def clause(kind: String)(c: Query) =
      copy(clauses = clauses + (kind -> (c :: clauses(kind))))
  }

  def bool(primary: Query) = Bool(primary)
}
