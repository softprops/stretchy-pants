package stretchypants

import org.json4s.{ JArray, JString, JValue, JNothing, JInt, JObject }
import org.json4s.JsonDSL._
import scala.concurrent.duration.FiniteDuration

sealed trait Facet {
  def asJson: JValue
}

/** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets.html */
object Facet {

  sealed trait Ordering {
    def value: String
  }

  object Ordering {
    abstract class Value(val value: String) extends Ordering
    case object Count extends Value("count")
    case object Term extends Value("term")
    case object ReverseCount extends Value("reverse_count")
    case object ReverseTerm extends Value("reverse_term")
  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets-terms-facet.html */
  case class Terms(
    _field: Option[List[String]] = None, // one = field, many = fields
    _scriptField: Option[String] = None,
    _size: Option[Int]           = None,
    _shardSize: Option[Int]      = None,
    _ordering: Option[Ordering]  = None,
    _allTerms: Option[Boolean]   = None,
    _exclude: List[String]       = Nil,
    _regex: Option[String]       = None,
    _regexFlags: Option[String]  = None,
    _script: Option[String] = None) extends Facet {

    def field(f: String*) = copy(_field = Some(f.toList))
    def scriptField(sf: String) = copy(_scriptField = Some(sf))
    def size(s: Int) = copy(_size = Some(s))
    def shardSize(s: Int) = copy(_shardSize = Some(s))
    def ordering(o: Ordering) = copy(_ordering = Some(o))
    def allTerms(at: Boolean) = copy(_allTerms = Some(at))
    def exclude(excl: String*) = copy(_exclude = excl.toList)
    def regex(re: String) = copy(_regex = Some(re))
    def regexFlags(rf: String) = copy(_regexFlags = Some(rf))
    def script(s: String) = copy(_script = Some(s))

    private[this] def primary =
      _field.map {
        case one :: Nil =>
          ("field"      -> JString(one))
        case many =>
          ("fields"     -> JArray(many.map(JString(_))))
      }.orElse(_scriptField.map { sf =>
        ("script_field" -> JString(sf))
      }).getOrElse(
        ("field"        -> JNothing)
      )

    def asJson: JValue =
      ("terms" ->
       primary ~
       ("size" -> _size) ~
       ("shard_size" -> _shardSize) ~
       ("all_terms" -> _allTerms) ~
       ("exclude" -> Some(_exclude).filter(_.nonEmpty)) ~
       ("regex" -> _regex) ~
       ("regex_flags" -> _regexFlags) ~
       ("script" -> _script) ~
       ("order" -> _ordering.map(_.value)))
  }

  def terms = Terms()

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets-range-facet.html */
  case class Range(
    _field: Option[String]                     = None,
    _keyValueFields: Option[(String, String)]  = None,
    _keyValueScripts: Option[(String, String)] = None,
    bounds: List[(Option[Int], Option[Int])]   = Nil) extends Facet {
    def field(f: String) = copy(_field = Some(f))
    def keyValueFields(key: String, value: String) =
      copy(_keyValueFields = Some(key, value))
    def keyValueScripts(key: String, value: String) =
      copy(_keyValueScripts = Some(key, value))
    def from(f: Int) = copy(bounds = (Some(f), None) :: bounds)
    def to(t: Int) = copy(bounds = (None, Some(t)) :: bounds)
    def range(f: Int, t: Int) = copy(bounds = (Some(f), Some(t)) :: bounds)
    def asJson = {
      val ranges = bounds.map { case (to, from) => ("to" -> to) ~ ("from" -> from) }
      ("range" ->
        (_field.map { fld => (fld -> ranges): JObject }.orElse {
        _keyValueFields.map {
          case (k,v) =>
            (("ranges"      -> ranges) ~
             ("key_field"   -> k) ~
             ("value_field" -> v): JObject)
        }
       }.orElse {
        _keyValueScripts.map {
          case (k,v) =>
            (("ranges"       -> ranges) ~
             ("key_script"   -> k) ~
             ("value_script" -> k): JObject)
        }
       }))
    }
  }

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets-histogram-facet.html */
  case class Histogram(
    _field: Option[String]                     = None,
    _keyValueFields: Option[(String, String)]  = None,
    _keyValueScripts: Option[(String, String)] = None,
    _scriptParams: Option[Map[String, String]] = None,
    _interval: Option[Int]                     = None,
    _timeInterval: Option[FiniteDuration]      = None) extends Facet {
    def field(f: String) = copy(_field = Some(f))
    def keyValueFields(key: String, value: String) =
      copy(_keyValueFields = Some(key, value))
    def keyValueScripts(key: String, value: String) =
      copy(_keyValueScripts = Some(key, value))
    def scriptParams(kv: (String, String)*) = copy(_scriptParams = Some(kv.toMap))
    def interval(i: Int) = copy(_interval = Some(i))
    def interval(d: FiniteDuration) = copy(_timeInterval = Some(d))
    def asJson = {
      ("histogram" ->
        (_field.map { fld =>
          (("field" -> fld) ~
           ("interval" -> _interval)): JObject
       }.orElse {
        _keyValueFields.map {
          case (k,v) =>
            (("interval" -> _interval) ~
             ("key_field"   -> k) ~
             ("value_field" -> v): JObject)
        }
       }.orElse {
        _keyValueScripts.map {
          case (k,v) =>
            (("interval" -> _interval) ~
             ("key_script"   -> k) ~
             ("value_script" -> k): JObject)
        }
       }))
    }
  }

  def histogram = Histogram()
}
