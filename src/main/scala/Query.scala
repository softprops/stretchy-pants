package stretchypants

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
      ("bool" -> (JObject() /: clauses) {
        case (obj, (k, cs)) => obj ~ (k -> cs.map(_.asJson))
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
      ("boosting" -> (JObject() /: clauses) {
        case (obj, (k, cs)) => obj ~ (k -> cs.map(_.asJson))
      } ~
      ("negative_boost" -> _negativeBoost))
    private def clause(kind: String)(q: Query) =
      copy(clauses + (kind -> (q :: clauses(kind))))
  }

  def boosting = Boosting()

  case class CommonTerms(
    _query: String,
    _cutoffFrequency: Option[Double] = None,
    _lowFreqOperator: Option[String] = None,
    _minimumShouldMatch: Option[Int] = None) extends Query {
    def query(q: String) = copy(_query = q)
    def cutoffFrequency(c: Double) = copy(_cutoffFrequency = Some(c))
    def lowFreqOperator(op: String) = copy(_lowFreqOperator = Some(op))
    def minimumShouldMatch(m: Int) = copy(_minimumShouldMatch = Some(m))
    def asJson =
      ("common" ->
       ("body" ->
        ("query" -> _query) ~
        ("cutoff_frequency" -> _cutoffFrequency) ~
        ("low_freq_operator" -> _lowFreqOperator) ~
        ("minimum_should_match" -> _minimumShouldMatch)))
  }

  def commonTerms(query: String) = CommonTerms(query)

  case class Dismatched(
    _queries: List[Query]       = Nil,
    _tieBreaker: Option[Double] = None,
    _boost: Option[Double]      = None) extends Query {
    def queries(q: Query*) = copy(_queries = q.toList)
    def tieBreaker(t: Double) = copy(_tieBreaker = Some(t))
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("dis_max" ->
       ("tie_breaker" -> _tieBreaker) ~
       ("boost" -> _boost) ~
       ("queries" -> _queries.map(_.asJson)))
  }

  def dismatched = Dismatched()

  // todo: filtered

  case class Fuzzy(
    query: String,
    _fields: List[String] = Nil,
    _maxQueryTerms: Option[Int] = None) extends Query {
    def fields(fs: String*) = copy(_fields = fs.toList)
    def maxQueryTerms(max: Int) = copy(_maxQueryTerms = Some(max))
    def asJson = 
      ("fuzzy_like_this" -> ("fields" -> _fields) ~ ("like_text" -> query) ~
       ("max_query_terms" -> _maxQueryTerms))
   }

  def fuzzy(query: String) = Fuzzy(query)

  case class FuzzyLike(
    _field: String,
    _query: Option[String]      = None,
    _ignoreTf: Option[Boolean]  = None,
    _maxQueryTerms: Option[Int] = None,
    _fuzziness: Option[Double]  = None,
    _prefixLength: Option[Int]  = None,
    _boost: Option[Double]      = None,
    _analyzer: Option[String]   = None) extends Query {

    def query(q: String) = copy(_query = Some(q))
    def ignoreTf(i: Boolean) = copy(_ignoreTf = Some(i))
    def maxQueryTerms(max: Int) = copy(_maxQueryTerms = Some(max))
    def fuzziness(f: Double) = copy(_fuzziness = Some(f))
    def prefixLength(pl: Int) = copy(_prefixLength = Some(pl))
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("fuzzy_like_this_field" ->
       (_field ->
        ("like_text"       -> _query) ~
        ("ignore_tf"       -> _ignoreTf) ~
        ("max_query_terms" -> _maxQueryTerms) ~
        ("fuzziness"       -> _fuzziness) ~
        ("prefix_length"   -> _prefixLength) ~
        ("boost"           -> _boost) ~
        ("analyzer"        -> _analyzer)))
  }

  def fuzzyLike(field: String) = FuzzyLike(field).query(_)

  case class MatchAll(
    _boost: Option[Double] = None) extends Query {
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("match_all" ->
        JObject() ~ ("boost" -> _boost))
  }

  def matchall = MatchAll()

  // todo: support string and date ranges
  case class Range(
    field: String,
    _gte: Option[Long] = None,
    _gt: Option[Long]  = None,
    _lte: Option[Long] = None,
    _lt: Option[Long]  = None,
    _boost: Option[Double] = None) extends Query {
    def gte(v: Long) = copy(_gte = Some(v))
    def gt(v: Long) = copy(_gt = Some(v))
    def lte(v: Long) = copy(_lte = Some(v))
    def lt(v: Long) = copy(_lt = Some(v))
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("range" ->
       (field ->
        ("gte" -> _gte) ~
        ("gt" -> _gt) ~
        ("lte" -> _lte) ~
        ("lt" -> _lt) ~
        ("boost" -> _boost)))
  }

  def range(field: String) = Range(field)

  case class Term(
    _term: (String, String),
    _boost: Option[Double] = None) extends Query {
    def boost(b: Double) = copy(_boost = Some(b))
    def term(t: (String, String)) = copy(_term = t)
    def asJson =
      _term match {
        case (field, value) =>
          ("term" ->
           (field ->
            ("value" -> value) ~ ("boost" -> _boost)))
      }
  }
  def term(t: (String, String)) = Term(t)

  case class Terms(
    _field: String,
    _values: List[String]            = Nil,
    _minimumShouldMatch: Option[Int] = None) extends Query {
    def minimumShouldMatch(min: Int) = copy(_minimumShouldMatch = Some(min))
    def values(v: String*) = copy(_values = v.toList)
    def asJson =
      ("terms" ->
       (_field -> _values) ~ ("minimum_should_match" -> _minimumShouldMatch))
  }
  def terms(field: String)(values: String*) = Terms(field).values(values:_*)
}
