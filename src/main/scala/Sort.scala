package stretchypants

sealed trait Order {
  def value: String
}
object Order {
  abstract class Value(val value: String) extends Order
  case object Asc extends Value("asc")
  case object Desc extends Value("desc")
}

object Sorting {
  sealed trait Mode {
    def value: String
  }
  object Mode {
    abstract class Value(val value: String) extends Mode
    case object Min extends Value("min")
    case object Max extends Value("max")
    case object Sum extends Value("sum")
    case object Avg extends Value("avg")
  }
}

// todo nested_filter
// todo missing
// todo ignore_unmapped
// todo _geo_distance
// todo geohash
// todo script
case class Sort(field: String, order: Option[Order] = None, mode: Option[Sorting.Mode] = None) {
  def asc = copy(order = Some(Order.Asc))
  def desc = copy(order = Some(Order.Desc))
}
