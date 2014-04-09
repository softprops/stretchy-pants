package stretchpants

import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._

trait Query {
  def asJson: JValue
}

object Query {
  /** The match family of queries does not go through a "query parsing" process. It does not support field name prefixes, wildcard characters, or other "adva    * nce" features. For this reason, chances of it failing are very small / non existent, and it provides an excellent behavior when it comes to just analyz    * e and run that text as a query behavior (which is usually what a text search box does). Also, the phrase_prefix type can provide a great "as you type"     * behavior to automatically load search results.
    *  http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
    */
  case class Matching(
    field: String, query: String,
    _operator: Option[String]        = None,
    _zeroTermsQuery: Option[Boolean] = None,
    _cutoffFreq: Option[Double]      = None,
    _type: Option[String]            = None,
    _analyzer: Option[String]        = None,
    // for use with phrase_prefix type
    _maxExpansions: Option[Int]      = None,
    _lenient: Option[Boolean]        = None) extends Query {
    def operator(op: String) = copy(_operator = Some(op))
    def and = operator("and")
    def or = operator("or")
    def cutoffFrequeny(c: Double) = copy(_cutoffFreq = Some(c))
    def phrase = copy(_type = Some("phrase"))
    def phrasePrefix = copy(_type = Some("phrase_prefix"))
    def analyzer(a: String) = copy(_analyzer = Some(a))
    def lenient(l: Boolean) = copy(_lenient = Some(l))
    def asJson =
      ("match" ->
         (field ->
          ("query" -> query) ~
          ("operator" -> _operator) ~
          ("zero_terms_query" ->
           _zeroTermsQuery.map( zt => if (zt) "all" else "none")) ~
          ("cutout_frequency" -> _cutoffFreq) ~
          ("type" -> _type) ~
          ("analyzer" -> _analyzer) ~
          ("max_expansions" -> _maxExpansions) ~
           ("lenient" -> _lenient)))
  }

  def matching(f: (String, String)) = Matching(f._1, f._2)

  case class MultiMatching(
    query: String,
    _fields: List[String]       = Nil,
    _type: Option[String]       = None,
    _tieBreaker: Option[Double] = None) extends Query {
    def fields(fs: String*) = copy(_fields = fs.toList)
    def bestFields = copy(_type = Some("best_fields"))
    def mostFields = copy(_type = Some("most_fields"))
    def crossFields = copy(_type = Some("cross_fields"))
    def phrase = copy(_type = Some("phrase"))
    def phrasePrefix = copy(_type = Some("phrase_prefix"))
    def asJson =
      ("multi_match" ->
       ("query" -> query) ~ ("fields" -> _fields) ~
       ("type" -> _type) ~ ("tie_breaker" -> _tieBreaker))
  }

  def multimatching(q: String) = MultiMatching(q)

  case class Bool(
    clauses: Map[String, List[Query]] =
      Map.empty[String, List[Query]].withDefaultValue(Nil),
    _minimumShoulds: Option[Int]    = None,
    _disableCoord: Option[Boolean] = None) extends Query {
    def must = clause("must")_
    def mustNot = clause("must_not")_
    def should = clause("should")_
    def minimumShoulds(m: Int) = copy(_minimumShoulds = Some(m))
    def disableCoord(d: Boolean) = copy(_disableCoord = Some(d))
    def asJson =
      ("bool" -> (JObject() /: clauses.map { case (k, qs) => (k, qs.map(_.asJson)) }) {
        case (obj, (k, cs)) => obj ~ (k -> cs)
      } ~
       ("minimum_should_match" -> _minimumShoulds) ~
       ("disable_coord" -> _disableCoord))
    private def clause(kind: String)(q: Query) =
      copy(clauses + (kind -> (q :: clauses(kind))))
   }

  def bool = Bool()

  case class Boosting(
    clauses: Map[String, List[Query]] =
      Map.empty[String, List[Query]].withDefaultValue(Nil),
    _negativeBoost: Option[Double] = None) extends Query {
    def positive = clause("positive")_
    def negative = clause("negative")_
    def negativeBoost(nb: Double) = copy(_negativeBoost = Some(nb))
    def asJson =
      ("boosting" -> (JObject() /: clauses.map { case (k, qs) => (k, qs.map(_.asJson)) }) {
        case (obj, (k, cs)) => obj ~ (k -> cs)
      } ~
       ("negative_boost" -> _negativeBoost))
    private def clause(kind: String)(q: Query) =
      copy(clauses + (kind -> (q :: clauses(kind))))
  }

  def boosting = Boosting()
}
