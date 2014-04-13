package stretchypants

/** http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html#_scoring_capabilities */
sealed trait ScoreType {
  def value: String
}

object ScoreType {
  abstract class Value(val value: String) extends ScoreType
  case object Max extends Value("max")
  case object Sum extends Value("sum")
  case object Avg extends Value("avg")
  case object None extends Value("none")
}
