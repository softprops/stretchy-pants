package stretchypants

import org.json4s.{ JArray, JString, JValue, JNothing, JInt, JObject }
import org.json4s.JsonDSL._

sealed trait Facet {
  def asJson: JValue
}

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

  val terms = Terms()

  case class Terms(
    _field: Option[List[String]] = None, // one = field, many = fields
    _scriptField: Option[String] = None,
    _size: Option[Int]           = None,
    _ordering: Option[Ordering]  = None,
    _allTerms: Option[Boolean]   = None,
    _exclude: List[String]       = Nil,
    _regex: Option[String]       = None,
    _regexFlags: Option[String]  = None,
    _script: Option[String] = None) extends Facet {

    def field(f: String*) = copy(_field = Some(f.toList))
    def scriptField(sf: String) = copy(_scriptField = Some(sf))
    def size(s: Int) = copy(_size = Some(s))
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
       ("all_terms" -> _allTerms) ~
       ("exclude" -> Some(_exclude).filter(_.nonEmpty)) ~
       ("regex" -> _regex) ~
       ("regex_flags" -> _regexFlags) ~
       ("script" -> _script) ~
       ("order" -> _ordering.map(_.value)))
  }
}
