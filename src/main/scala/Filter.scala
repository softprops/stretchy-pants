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

  case class Exists(
    field: String, _value: Option[String] = None) extends Filter {
    def value(v: String) = copy(_value = Some(v))
    def asJson =
      ("constant_score" ->
       ("filter" ->
        ("exists" ->
         (field -> _value))))
  }

  def exists(field: String) = Exists(field).value(_)

  case class BoundedBox(
    q: Query, _field: Option[String] = None,
    _cache: Option[Boolean] = None,
    _box: Option[(String, String)] = None) extends Filter {
    def field(f: String) = copy(_field = Some(f))
    /** (topLeftLat, topLeftLon, bottomRightLat, bottomRightLon) */
    def box(b: (Double, Double, Double, Double)) =
      copy(_box = Some((s"${b._1}, ${b._2}", s"${b._3}, ${b._4}")))
    def geohash(b: (String, String)) =
      copy(_box = Some(b))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("geo_bounding_box" ->
         _field.map { fld =>
           (fld -> _box.map {
             case (topLeft, botRight) =>
               ("top_left" -> topLeft) ~
               ("bottom_right" -> botRight)
           }) ~
           ("_cache" -> _cache)
         })))
  }

  def boundedBox(q: Query) = BoundedBox(q).field(_)

  case class GeoDistance(
    q: Query,
    _field: Option[String]         = None,
    _distance: Option[String]      = None,
    _location: Option[String]      = None,
    _distanceType: Option[String]  = None,
    _optimizeBBox: Option[Boolean] = None,
    _cache: Option[Boolean]        = None) extends Filter {
    def field(f: String) = copy(_field = Some(f))
    def location(l: (Double, Double)) =
      copy(_location = Some(s"${l._1}, ${l._2}"))
    def geohash(g: String) = copy(_location = Some(g))
    /** units defined in http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/common-options.html#distance-units */
    def distance(d: String) = copy(_distance = Some(d))
    def distanceType(d: String) = copy(_distanceType = Some(d))
    def optimizeBBox(b: Boolean) = copy(_optimizeBBox = Some(b))
    def cache(c: Boolean) = copy(_cache = Some(c))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("geo_distance" ->
         ("distance_type" -> _distanceType) ~
         ("optimize_bbox" -> _optimizeBBox) ~
         ("distance" -> _distance) ~
         _field.map { fld =>
           (fld -> _location)
         }.getOrElse(("x" -> None)))))
  }

  def geodistance(q: Query) = GeoDistance(q).field(_)


  case class GeoRange(
    q: Query,
    _field: Option[String]    = None,
    _location: Option[String] = None,
    _from: Option[String]     = None,
    _to: Option[String]       = None) extends Filter {
    def field(f: String) = copy(_field = Some(f))
    def from(f: String) = copy(_from = Some(f))
    def to(t: String) = copy(_to = Some(t))
    def geohash(g: String) = copy(_location = Some(g))
    def location(l: (Double, Double)) =
      copy(_location = Some(s"${l._1}, ${l._2}"))
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("geo_distance_range" ->
         ("from" -> _from) ~
         ("to"   -> _to) ~
         _field.map { fld =>
           (fld -> _location)
         }.getOrElse(("x" -> None)))))
  }
  def georange(q: Query) = GeoRange(q).field(_)
}
