package stretchypants

import org.json4s.JsonDSL._
import org.json4s.{ JObject, JValue }

trait Filter {
  def asJson: JValue
}

/* http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-filters.html */
object Filter {

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-and-filter.html */
  case class And(
    q: Query,
    ands: List[Query]       = Nil,
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-bool-filter.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html */
  case class Exists(
    field: String,
    _value: Option[String] = None) extends Filter {
    def value(v: String) = copy(_value = Some(v))
    def asJson =
      ("constant_score" ->
       ("filter" ->
        ("exists" ->
         (field -> _value))))
  }

  def exists(field: String) = Exists(field).value(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-bounding-box-filter.html */
  case class BoundedBox(
    q: Query,
    _field: Option[String]         = None,
    _cache: Option[Boolean]        = None,
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-filter.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-range-filter.html */
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

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-polygon-filter.html */
  case class GeoPolygon(
    q: Query,
    _field: Option[String]  = None,
    _points: List[String]   = Nil,
    _cache: Option[Boolean] = None) extends Filter {
    def cache(c: Boolean) = copy(_cache = Some(c))
    def field(f: String) = copy(_field = Some(f))
    def points(p: (Double, Double)*) =
      copy(_points = p.toList.map {
        case (lat, lon) => s"$lat, $lon"
      })
    def geohashes(h: String*) =
      copy(_points = h.toList)
    def point(p: (Double, Double)) =
      copy(_points  = s"${p._1}, ${p._2}" :: _points)
    def geohash(h: String) =
      copy(_points  = h :: _points)
    def asJson =
      ("filtered" ->
       ("query" -> q.asJson) ~
       ("filter" ->
        ("geo_polygon" ->
         ("_cache" -> _cache) ~
         _field.map { fld =>
           (fld -> Some(("points" -> _points)))
         }.getOrElse(("x" -> None)))))
  }

  def geoPolygon(q: Query) = GeoPolygon(q).field(_)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-filter.html */
  
  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geohash-cell-filter.html */
  
  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-child-filter.html */
  case class HasChild(
    typ: String,
    _query: Option[Query]   = None,
    _filter: Option[Filter] = None) extends Filter {
    def query(q: Query) = copy(_query = Some(q))
    def filter(f: Filter) = copy(_filter = Some(f))
    def asJson =
      ("has_child" ->
       ("type" -> typ) ~
       ("filter" -> _filter.map(_.asJson)) ~
       ("query" -> _query.map(_.asJson)))
  }

  def hasChild(typ: String) = HasChild(typ)


  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-parent-filter.html */
  case class HasParent(
    typ: String,
    _query: Option[Query]   = None,
    _filter: Option[Filter] = None) extends Filter {
    def query(q: Query) = copy(_query = Some(q))
    def filter(f: Filter) = copy(_filter = Some(f))
    def asJson =
      ("has_parent" ->
       ("type" -> typ) ~
       ("filter" -> _filter.map(_.asJson)) ~
       ("query" -> _query.map(_.asJson)))
  }

  def hasParent(typ: String) = HasParent(typ)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-ids-filter.html#query-dsl-ids-filter */

  case class Ids(
    typ: String,
    _ids: List[String] = Nil) extends Filter {
    def ids(i: String*) = copy(_ids = i.toList)
    def asJson =
      ("ids" -> ("type" -> typ) ~ ("values" -> _ids))
  }

  def ids(typ: String) = Ids(typ)

  /** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-indices-filter.html */
}
