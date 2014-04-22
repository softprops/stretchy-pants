package stretchypants

import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._

trait Query {
  def asJson: JValue
}

/** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-queries.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-common-terms-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html */
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

  class Filtered(
    q: Query,
    _filter: Option[Filter]) extends Query {
    def filter(f: Filter) = copy(_filter = Some(f))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" -> _filter.map(_.asJson)))
  }

  def filtered(q: Query) = Filtered(q)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-flt-query.html */
  case class FuzzyLikeThis(
    query: String,
    _fields: List[String] = Nil,
    _maxQueryTerms: Option[Int] = None) extends Query {
    def fields(fs: String*) = copy(_fields = fs.toList)
    def maxQueryTerms(max: Int) = copy(_maxQueryTerms = Some(max))
    def asJson = 
      ("fuzzy_like_this" -> ("fields" -> _fields) ~ ("like_text" -> query) ~
       ("max_query_terms" -> _maxQueryTerms))
  }

  def fuzzyLikeThis(query: String) = FuzzyLikeThis(query)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-flt-field-query.html */
  case class FuzzyLikeThisField(
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

  def fuzzyLikeThisField(field: String) = FuzzyLikeThisField(field).query(_)

  // todo: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html

  /**  http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html */
  case class Fuzzy(
    _field: String,
    _value: Option[String] = None,
    _boost: Option[Double] = None,
    _fuzziness: Option[String] = None,
    _prefixLength: Option[Int] = None,
    _maxExpansions: Option[Int] = None) extends Query {
    def value(v: String) = copy(_value = Some(v))
    def boost(b: Double) = copy(_boost = Some(b))
    /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/common-options.html#fuzziness */
    def fuzziness(f: String) = copy(_fuzziness = Some(f))
    def prefixLength(p: Int) = copy(_prefixLength = Some(p))
    def maxExpansions(m: Int) = copy(_maxExpansions = Some(m))
    def asJson =
      ("fuzzy" ->
       (_field ->
        ("value" -> _value) ~
        ("boost" -> _boost) ~
        ("fuzziness" -> _fuzziness) ~
        ("prefix_length" -> _prefixLength) ~
        ("max_expansions" -> _maxExpansions)))
  }

  def fuzzy(field: String) = Fuzzy(field).value(_)

  /*case class GeoShape(
    _field: String) extends Query {

  }

  def geoshape(field: String) = GeoShape(field).*/

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html */
  case class HasChild(
    _kind: String,
    _q: Option[Query] = None,
    _scoreType: Option[ScoreType] = None) extends Query {
    def query(q: Query) = copy(_q = Some(q))
    def scoreType(s: ScoreType) = copy(_scoreType = Some(s))
    def asJson =
      ("has_child" ->
       ("type" -> _kind) ~
       ("score_type" -> _scoreType.map(_.value)) ~
       ("query" -> _q.map(_.asJson)))
  }

  def hasChild(kind: String) = HasChild(kind).query(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-parent-query.html */
  case class HasParent(
    _kind: String,
    _q: Option[Query] = None,
    /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-parent-query.html#_scoring_capabilities_2 */
    _scoreType: Option[String] = None) extends Query {
    def query(q: Query) = copy(_q = Some(q))
    def score = scoreType("score")
    def scoreType(s: String) = copy(_scoreType = Some(s))
    def asJson =
      ("has_parent" ->
       ("type" -> _kind) ~
       ("score_type" -> _scoreType) ~
       ("query" -> _q.map(_.asJson)))
  }

  def hasParent(kind: String) = HasParent(kind).query(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-ids-query.html */
  case class Ids(
    _ids: List[String],
    _kind: Option[String] = None) extends Query {
    def ids(i: String*) = copy(_ids = i.toList)
    def kind(k: String) = copy(_kind = Some(k))
    def asJson =
      ("ids" ->
       ("type" -> _kind) ~
       ("values" -> _ids))
  }

  def ids(values: String*) = Ids(values.toList)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-indices-query.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html */
  case class MatchAll(
    _boost: Option[Double] = None) extends Query {
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("match_all" ->
        JObject() ~ ("boost" -> _boost))
  }

  def matchall = MatchAll()

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html */
  case class MoreLikeThis(
    _query: String,
    _fields: List[String] = Nil,
    _percentTermsToMatch: Option[Double] = None,
    _minTermFreq: Option[Int] = None,
    _maxQueryTerms: Option[Int] = None,
    _stopWords: List[String] = Nil,
    _minDocFreq: Option[Int] = None,
    _maxDocFreq: Option[Int] = None,
    _minWordLength: Option[Int] = None,
    _boostTerms: Option[Int] = None,
    _boost: Option[Double] = None,
    _analyzer: Option[String] = None) extends Query {
    def fields(fs: String*) = copy(_fields = fs.toList)
    def asJson =
      ("more_like_this" ->
       ("fields" -> Some(_fields).filter(_.nonEmpty)) ~
       ("like_text"              -> _query) ~
       ("percent_terms_to_match" -> _percentTermsToMatch) ~
       ("min_term_freq"          -> _minTermFreq) ~
       ("max_query_terms"        -> _maxQueryTerms) ~
       ("stop_words"             -> Some(_stopWords).filter(_.nonEmpty)) ~
       ("min_doc_freq"           -> _minDocFreq) ~
       ("max_doc_freq"           -> _maxDocFreq) ~
       ("min_word_length"        -> _minWordLength) ~
       ("boost_terms"            -> _boostTerms) ~
       ("boost"                  -> _boost) ~
       ("analyzer"               -> _analyzer))
  }

  def moreLikeThis(query: String) = MoreLikeThis(query)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-mlt-field-query.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html */
  case class Prefix(
    _field: String,
    _value: Option[String] = None,
    _boost: Option[Double] = None) extends Query {
    def boost(b: Double) = copy(_boost = Some(b))
    def value(v: String) = copy(_value = Some(v))
    def asJson =
      ("prefix" ->
       (_field ->
        ("value" -> _value) ~
        ("boost" -> _boost)))
  }

  def prefix(field: String) = Prefix(field).value(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html */
  case class QueryString(
    q: String,
    _defaultField: Option[String]     = None,
    _defaultOperation: Option[String] = None) extends Query {
    def defaultField(d: String) = copy(_defaultField = Some(d))
    def defaultOperation(d: String) = copy(_defaultOperation = Some(d))
    def and = defaultOperation("AND")
    def or = defaultOperation("OR")
    def asJson =
      ("query_string" ->
       ("default_field" -> _defaultField) ~
       ("default_operation" -> _defaultOperation) ~
       ("query" -> q))
  }

  def queryString(q: String) = QueryString(q)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html */
  case class SimpleQueryString(
    q: String,
    _defaultOperator: Option[String] = None,
    _fields: List[String]            = Nil) extends Query {
    def fields(f: String*) = copy(_fields = f.toList)
    def defaultOperator(d: String) = copy(_defaultOperator = Some(d))
    def or = defaultOperator("OR")
    def and = defaultOperator("AND")
    def asJson =
      ("simple_query_string" ->
       ("default_operator" -> _defaultOperator) ~
       ("fields" -> Some(_fields).filter(_.nonEmpty)) ~
       ("query" -> q))
  }

  def simpleQueryString(q: String) = SimpleQueryString(q)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html */
  case class Regex(
    _field: String,
    _value: Option[String] = None,
    _flags: List[String]   = Nil,
    _boost: Option[Double] = None) extends Query {
    def value(v: String) = copy(_value = Some(v))
    def flags(f: String*) = copy(_flags = f.toList)
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("regexp" ->
       (_field ->
        ("value" -> _value) ~
        ("boost" -> _boost) ~
        ("flags" -> Some(_flags).filter(_.nonEmpty).map(_.mkString("|")))))
  }

  def regex(field: String) = Regex(field).value(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-first-query.html */
  case class SpanFirst(
    _field: String,
    _value: Option[String] = None,
    _end: Option[Int] = None) extends Query {
    def end(e: Int) = copy(_end = Some(e))
    def value(v: String) = copy(_value = Some(v))
    def asJson =
      ("span_first" ->
       ("end" -> _end) ~
       ("match" ->
        ("span_term" ->
         (_field -> _value))))
  }

  def spanFirst(field: String) = SpanFirst(field).value(_)
  
  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-multi-term-query.html */
  case class SpanMulti(q: Query) extends Query {
    def asJson =
      ("span_multi" ->
       ("match" -> q.asJson))
  }

  /** query should be one of fuzzy, prefix, term range or regexp query */
  def spanMulti(q: Query) = SpanMulti(q)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-near-query.html */
  case class SpanNear(
    _terms: List[(String, String)] = Nil,
    _slop: Option[Int] = None,
    _inOrder: Option[Boolean] = None,
    _collectPayloads: Option[Boolean] = None) extends Query {
    def terms(t: (String, String)*) = copy(_terms = t.toList)
    def term(t: (String, String)) = copy(_terms = t :: _terms)
    def slop(s: Int) = copy(_slop = Some(s))
    def inOrder(b: Boolean) = copy(_inOrder = Some(b))
    def collectPayloads(c: Boolean) = copy(_collectPayloads = Some(c))
    def asJson =
      ("span_near" ->
       ("clauses" -> _terms.map {
         case (key, value) => ("span_term"  -> (key -> value))           
       }) ~
       ("slop" -> _slop) ~
       ("in_order" -> _inOrder) ~
       ("collect_payloads" -> _collectPayloads))
  }

  def spanNear = SpanNear()

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-not-query.html */
  case class SpanNot(
    _include: Option[(String, String)] = None,
    _exclude: Option[(String, String)] = None) extends Query {
    def include(i: (String, String)) = copy(_include = Some(i))
    def exclude(e: (String, String)) = copy(_exclude = Some(e))
    def asJson =
      ("span_not" ->
       ("include" -> _include.map {
         case (key, value) => (key -> value)
       }) ~
       ("exclude" -> _exclude.map {
         case (key, value) => (key -> value)
       }))
  }

  def spanNot = SpanNot()

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-or-query.html */
  case class SpanOr(
    _terms: List[(String, String)] = Nil) extends Query {
    def terms(t: (String, String)*) = copy(_terms = t.toList)
    def term(t: (String, String)) = copy(_terms = t :: _terms)
    def asJson =
      ("span_or" ->
       ("clauses" -> _terms.map {
         case (key, value) => ("span_term"  -> (key -> value))           
       }))
  }

  def spanOr = SpanOr()

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-span-term-query.html */
  case class SpanTerm(
    _field: String,
    _value: Option[String] = None,
    _boost: Option[Double] = None) extends Query {
    def value(v: String) = copy(_value = Some(v))
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("span_term" ->
       (_field ->
        ("value" -> _value) ~
        ("boost" -> _boost)))
  }

  def spanTerm(field: String) = SpanTerm(field)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-top-children-query.html */
  
  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html */
  case class Wildcard(
    _field: String,
    _value: Option[String] = None,
    _boost: Option[Double] = None) extends Query {
    def value(v: String) = copy(_value = Some(v))
    def boost(b: Double) = copy(_boost = Some(b))
    def asJson =
      ("wildcard" ->
       (_field ->
        ("value" -> _value) ~
        ("boost" -> _boost)))
  }

  def wildcard(field: String) = Wildcard(field)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-minimum-should-match.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-multi-term-rewrite.html */

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-template-query.html */

  
}
