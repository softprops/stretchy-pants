package stretchypants

sealed trait Preference {
  def value: String
}

object Preference {
  abstract class Value(val value: String) extends Preference
  case object Primary extends Value("_primary")
  case object PrimaryFirst extends Value("_primary_first")
  case object Local extends Value("_local")
  case class Only(node: String) extends Value(s"_only_node:$node")
  case class Prefer(node: String) extends Value(s"_prefer_node:$node")
  case class Shards(shards: String*) extends Value(s"_shards:${shards.mkString(",")}")
  case class Custom(val value: String) extends Preference
}
