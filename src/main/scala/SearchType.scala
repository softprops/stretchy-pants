package stretchypants

sealed trait SearchType {
  def value: String
}
object SearchType {
  abstract class Value(val value: String) extends SearchType
  case object DfsQueryThenFetch extends Value("dfs_query_then_fetch")
  case object DfsQueryAndFetch extends Value("dfs_query_and_fetch")
  case object QueryThenFetch extends Value("query_then_fetch")
  case object QueryAndFetch extends Value("query_and_fetch")
  case object Count extends Value("count")
  case object Scan extends Value("scan")
}
